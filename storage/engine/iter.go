
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
type ECSState struct {
	key		MVCCKey;
	value	[]byte;
}

func ecsIterSeek(SK MVCCKey) ECSState {
	if(keyList.Len() == 0) {
		fmt.Println("keyList is empty")
		getList()
	}
	//fmt.Println(keyList.Len())
	//fmt.Println(SAMPLE_KEY, SAMPLE_KEY_TIME, "is passed")
	i := 0
	fmt.Println("called for", SK)
	for e := keyList.Front(); e != nil; e = e.Next() {
		key := e.Value.(MVCCKey)
		/*fmt.Println("comparing :")
		fmt.Println(SK)
		fmt.Println(key)
		fmt.Println()*/
		if(SK.Equal(key) || SK.Lower(key)) {
			//fmt.Println("Answer is", key)
			return ECSState{
				key: key,
				value: []byte("abc"),
			}
		}
		i++
	}
	//fmt.Println("Answer is .......")
	return ECSState{

		value: []byte("not found"),
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

			//keyByte := fmt.Sprintf("%s", segs[1])
			//fmt.Println(len(segs[1]))
			mvccKey = MVCCKey{
				Key:       []byte(segs[1]),
				Timestamp: hlc.Timestamp{
					WallTime: int64(wt),
					Logical:  int32(lt),
				},
			}
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


