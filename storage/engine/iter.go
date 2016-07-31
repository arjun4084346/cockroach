
package engine

import (
	"fmt"
	"encoding/hex"
	"container/list"
	"os"

	"github.com/biogo/store/interval"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/roachpb"
)

var keyList = list.New()

type ECSIterState struct {
	key		MVCCKey;
	value	[]byte;
	valid	bool
}

/*func printKey(key MVCCKey) {
	l := len(key.Key)
	var i int
	for i=0; i<l; i++ {
		fmt.Printf("%s", key.Key[i])
	}
	fmt.Printf("\n%d chars long\n", l)
}*/

func ECSIterSeek(SK MVCCKey, prefix bool, debug bool) ECSIterState {
	getList(goToECSKey(SK), prefix, SK, debug)
	for e := keyList.Front(); e != nil; e = e.Next() {
		key := e.Value.(MVCCKey)
		if !key.IsValue() {
			continue
		}
		if debug {
			fmt.Println("comparing", SK, "to", key)
		}
		if(SK.Equal(key) || SK.Less2(key)) {
			if debug {
				fmt.Println("found", key)
			}
			//continue
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
	f, _ := os.OpenFile("log", os.O_WRONLY, 0600)
	defer f.Close()
	s3Contents := output.Contents
	keyList.Init()
	for _, s3Content := range s3Contents {
		key, _ := hex.DecodeString(*s3Content.Key)
		var mvccKey MVCCKey
		if(len(key) > 12) {
		//fmt.Println(len(key), key)
			keybuf := key[:len(key)-12]
			/*keybuf := key[:len(key)-29]
			wt, _ := strconv.ParseInt(string(key[len(key)-29:len(key)-10]), 10, 64)
			lt, _ := strconv.ParseInt(string(key[len(key)-10:]), 10, 32)*/

			ts := TSinGo2(key[len(key)-12:])
			mvccKey = MVCCKey{
				Key:       keybuf,
				/*Timestamp: hlc.Timestamp{
					WallTime: int64(wt),
					Logical:  int32(lt),
				},*/
				Timestamp : ts,
			}
			/*unsafeKey := getGo(&segs[1], lens)
			safeKey := make([]byte, len(unsafeKey), len(unsafeKey)+1)
			copy(safeKey, unsafeKey)*/
		} else {
			mvccKey = MVCCKey{
				Key:       key,
			}
		}
		//fmt.Println("key formatted", mvccKey)
		//fmt.Printf("Key formatted %v\n\n", mvccKey)
		_, _ = f.WriteString(mvccKey.String() + "\n")
		f.Sync()
		keyList.PushBack(mvccKey)
	}
	if(debug) {
		fmt.Printf("%d keys, prefix %v, key %s % x\n", keyList.Len(), prefix, SK, prefixKey)
	}
}
