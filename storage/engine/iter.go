
package engine

import (
	"fmt"
	"encoding/hex"
	"regexp"
	"strconv"

	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"container/list"
	"os"
)

var keyList = list.New()
type ECSIterState struct {
	key		MVCCKey;
	value	[]byte;
	valid	bool
}

func ecsIterSeek(SK MVCCKey) ECSIterState {
	if(keyList.Len() == 0) {
		fmt.Println("keyList is empty")
		getList()
	}
	i := 0
	fmt.Println("called 2 for  ", SK)
	for e := keyList.Front(); e != nil; e = e.Next() {
		key := e.Value.(MVCCKey)
		if(SK.myEqual(key) || SK.Lower(key)) {	//fix myEqual too, maybe
			return ECSIterState{
				key:		key,
				value:	[]byte("abc"),
				valid:	true,
			}
		}
		i++
	}
	return ECSIterState{
		valid:	false,
	}
}

func getList() {
	r, _ := regexp.Compile(`(.*)/(.*),(.*)`)
	//s, _ := regexp.Compile(`(.*).(.*),(.*)`)
	//_ = hex.EncodeToString([]byte(key))
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))
	output, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(BUCKET),
	})

	if err != nil {
		fmt.Println("error!!!", err.Error())
		return
	}
	f, _ := os.OpenFile("log", os.O_WRONLY, 0600)
	defer f.Close()

		//_, _ = f.WriteString(iter.Key().String() + "\n" + unsafeKey.String() + "\n\n")


	s3Contents := output.Contents
	for _, s3Content := range s3Contents {
		key, _ := hex.DecodeString(*s3Content.Key)
		//fmt.Println(string(key))
		segs := r.FindStringSubmatch(string(key))
		var mvccKey MVCCKey
		if(len(segs) > 0) {
			//segs := s.FindStringSubmatch(segs[2])
			wt, _ := strconv.ParseInt(segs[2], 10, 64)
			lt, _ := strconv.ParseInt(segs[3], 10, 32)
			lens := len(segs[1])
			seg1 := key[0:lens]
			//keyByte := fmt.Sprintf("%s", segs[1])
			//fmt.Println(len(segs[1]))

			mvccKey = MVCCKey{
				Key:       seg1,
				Timestamp: hlc.Timestamp{
					WallTime: int64(wt),
					Logical:  int32(lt),
				},
			}
			//fmt.Println(seg1, len(seg1), mvccKey.StringWithoutQuote(), len(mvccKey.Key))
			//_, _ = f.WriteString(segs[1])
			//fmt.Println(len(mvccKey.Key.StringWithoutQuote()))
		} else {
			mvccKey = MVCCKey{
				Key:       key,
				//Timestamp: hlc.Timestamp{
					//WallTime: int64(segs[2]),
					//Logical:  int32(key.logical),
				//},
			}
			//_, _ = f.WriteString(string(key))
		}
		_, _ = f.WriteString(mvccKey.String() + "\n")
		f.Sync()
		keyList.PushBack(mvccKey)
	}
	/*for e := keyList.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}*/
}


