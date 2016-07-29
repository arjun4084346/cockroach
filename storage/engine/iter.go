
package engine

import (
	"fmt"
	"encoding/hex"
	"strconv"
	"container/list"
	"os"

	"github.com/cockroachdb/cockroach/util/hlc"
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

func ecsIterSeek(SK MVCCKey) ECSIterState {
	if(keyList.Len() == 0) {
		fmt.Println("keyList is empty")
		getList()
	}
	i := 0
	for e := keyList.Front(); e != nil; e = e.Next() {
		key := e.Value.(MVCCKey)
		if(SK.Less2(key)) {
			//fmt.Println("found", key)
			return ECSIterState{
				key:		key,
				value:	[]byte("abc"),
				valid:	true,
			}
		}
		i++
	}
	//fmt.Println()

	return ECSIterState{
		valid:	false,
	}
}

func isSmall2(key1, key2 roachpb.Key) bool {
	//var ba1 []byte
	//var ba2 []byte
	//ba1 = key1.Key
	//ba2 = key2.Key
	//len1 := len(ba1.(roachpb.Key))
	//len2 := len(ba2.(roachpb.Key))
	len1 := len(key1)
	len2 := len(key2)
	a := 0
	fmt.Println("lengths are ", len1, len2)
	//fmt.Println("Strings2 are---", string(ba1), "---", string(ba2), "---")
	fmt.Println("Strings1 are---", key1, "---", key2, "---")
	for ; len1>0 && len2>0;  {
		fmt.Println("Strings2 are---", key1[a], "---", key2[a], "---")
		//fmt.Println("function returning", bytes.Compare(key1, key2))
		/*if(key1[a] > key2[a]) {
			return false
		}
		if(key1[a] < key2[a]) {
			return true
		}*/
		a++
		len1--
		len2--
	}
	/*if(len1 == 0 && len2 == 0) {
		return key1.Timestamp.Less(key2.Timestamp)
	}*/
	if(len1 == 0) {
		return true
	}
	return false
}

func isSmall(ba1 interval.Comparable, ba2 interval.Comparable) bool {
	//var ba1 []byte
	//var ba2 []byte
	//ba1 = key1.Key
	//ba2 = key2.Key
	len1 := len(ba1.(roachpb.Key))
	len2 := len(ba2.(roachpb.Key))
	a := 0
	//fmt.Println("lengths are ", len1, len2)
	//fmt.Println("Strings2 are---", string(ba1), "---", string(ba2), "---")
	fmt.Println("Strings1 are---", ba1.(roachpb.Key), "---", ba2.(roachpb.Key), "---")
	for ; len1>0 && len2>0;  {
		fmt.Println("Strings2 are---", ba1.(roachpb.Key)[a], "---", ba2.(roachpb.Key)[a], "---")
		fmt.Println("function returning", ba1.(roachpb.Key).Compare(ba2.(roachpb.Key)))
		if(ba1.(roachpb.Key)[a] > ba2.(roachpb.Key)[a]) {
			return false
		}
		if(ba1.(roachpb.Key)[a] < ba2.(roachpb.Key)[a]) {
			return true
		}
		a++
		len1--
		len2--
	}
	/*if(len1 == 0 && len2 == 0) {
		return key1.Timestamp.Less(key2.Timestamp)
	}*/
	if(len1 == 0) {
		return true
	}
	return false
}

func getList() {
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
	s3Contents := output.Contents
	keyList.Init()
	for _, s3Content := range s3Contents {
		key, _ := hex.DecodeString(*s3Content.Key)
		//fmt.Printf("decoded string %v\n", key)
		var mvccKey MVCCKey
		if(len(key) > 29) {
			keybuf := key[:len(key)-29]
			wt, _ := strconv.ParseInt(string(key[len(key)-29:len(key)-10]), 10, 64)
			lt, _ := strconv.ParseInt(string(key[len(key)-10:]), 10, 32)
			mvccKey = MVCCKey{
				Key:       keybuf,
				Timestamp: hlc.Timestamp{
					WallTime: int64(wt),
					Logical:  int32(lt),
				},
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
}
