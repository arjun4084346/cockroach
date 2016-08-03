
package engine

import (
	"fmt"
	"encoding/hex"
	"container/list"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/util/hlc"
)

var keyList = list.New()
var keyList2 = list.New()

type ECSIterState struct {
	key		MVCCKey;
	value	[]byte;
	valid	bool
}

func ECSIterSeek(SK MVCCKey, prefix bool, debug bool) ECSIterState {
	getList(goToECSKey(SK), prefix, SK, debug)
	for e := keyList.Front(); e != nil; e = e.Next() {
		key := e.Value.(MVCCKey)
		if (prefix || SK.IsValue()) && !key.IsValue() {
			fmt.Println("skipping", key)
			continue
		}
		if debug {
			fmt.Println("comparing", SK, "to", key)
		}
		if key.Key.StartsWith(SK.Key) {
			secondaryECSIterState := secondaryECSIterSeek(key, SK.Timestamp, debug)
			if !secondaryECSIterState.valid {
				continue
			}
			return secondaryECSIterState
		}
		if c := SK.Key.Compare(key.Key); c < 0 {		// c == 0 should have covered in case above
			return ECSIterState{	// c < 0 i.e. SK < key
				key:		key,
				value:	[]byte("abc"),
				valid:	true,
			}
		}
	}
	return ECSIterState{
		valid:	false,
	}
}

func secondaryECSIterSeek(prefixKey MVCCKey, seekKeyTS hlc.Timestamp, debug bool) ECSIterState {
	newSK := MVCCKey{
		Key:				prefixKey.Key,
		//Timestamp:	SK.Timestamp,
	}
	if debug {
		fmt.Println("Sending new Key", newSK)
	}
	getList2(goToECSKey(newSK), true, prefixKey, debug)
	for e := keyList2.Back(); e != nil; e = e.Prev() {
		key := e.Value.(MVCCKey)
		if debug {
			fmt.Println("SECONDARY comparing", newSK, "to", key, seekKeyTS)
		}
		if(key.Timestamp.Less2(seekKeyTS)) {
			if debug {
				fmt.Println("SECONDARY found", key)
			}
			return ECSIterState{
				key:		key,
				value:	[]byte("abc"),
				valid:	true,
			}
		}
	}
	return ECSIterState{
		valid:	false,
	}
}

func getList2(prefixKey []byte, prefix bool, SK MVCCKey, debug bool) {
	//fmt.Printf("% x\n", prefixKey)
	keyStr := hex.EncodeToString(prefixKey)
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))
	var output *s3.ListObjectsOutput
	var err error
		output, err = svc.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(BUCKET),
			Prefix: aws.String(keyStr),
		})

	if err != nil {
		fmt.Println("error!!!", err.Error())
		return
	}
	f, _ := os.OpenFile("log2", os.O_APPEND | os.O_WRONLY, 0600)
	defer f.Close()
	s3Contents := output.Contents
	keyList2.Init()
	for _, s3Content := range s3Contents {
		key, _ := hex.DecodeString(*s3Content.Key)
		var mvccKey MVCCKey
		if(len(key) > 12) {
			keybuf := key[:len(key)-12]
			ts := TSinGo2(key[len(key)-12:])
			mvccKey = MVCCKey{
				Key:       keybuf,
				Timestamp : ts,
			}
		} else {
			mvccKey = MVCCKey{
				Key:       key,
			}
		}
			_, _ = f.WriteString(mvccKey.String() + "\n")
			f.Sync()
		keyList2.PushBack(mvccKey)
	}
	_, _ = f.WriteString("\n")
	if(debug) {
		fmt.Printf("Secondary : %d keys, key %s % x\n", keyList2.Len(), SK, prefixKey)
	}
}

func getList(prefixKey []byte, prefix bool, SK MVCCKey, debug bool) {
	keyStr := hex.EncodeToString(prefixKey)
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))
	var output *s3.ListObjectsOutput
	var err error
	if(prefix) {
		output, err = svc.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(BUCKET),
			Prefix: aws.String(keyStr),
		})
	} else {
		output, err = svc.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(BUCKET),
		})
	}

	if err != nil {
		fmt.Println("error!!!", err.Error())
		return
	}
	f, _ := os.OpenFile("log", os.O_APPEND | os.O_WRONLY, 0600)
	defer f.Close()
	s3Contents := output.Contents
	keyList.Init()
	for _, s3Content := range s3Contents {
		key, _ := hex.DecodeString(*s3Content.Key)
		var mvccKey MVCCKey
		l := len(key)
		if(l > 12) {
			//keybuf := make([]byte, l, l+1)
			keybuf := key[:l-12]
			//copy(keybuf, key[:l-12])
			ts := TSinGo2(key[l-12:])
			mvccKey = MVCCKey{
				Key:       keybuf,
				Timestamp : ts,
			}
		} else {
			mvccKey = MVCCKey{
				Key:       key,
			}
		}
		_, _ = f.WriteString(mvccKey.String() + "\n")
		f.Sync()
		keyList.PushBack(mvccKey)
	}
	_, _ = f.WriteString("\n")
	if(debug) {
		fmt.Printf("%d keys, prefix %v, key %s % x\n", keyList.Len(), prefix, SK, prefixKey)
	}
}
