package engine

import (
	"fmt"
	"io"
	"strings"
	"encoding/hex"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"bytes"
)

const BUCKET string = "b1"
const ENDPOINT string = "http://10.247.78.204:9020"

func qualifiedKey(keyStr string) bool {
	//return false		//COMMENT THIS LINE TO EXECUTE CHANGES
	if(!(strings.Contains(keyStr, "/Table/11") || strings.Contains(keyStr, "/Table/14")  ||			//lease && ui
	strings.Contains(keyStr, "/Table/12") || strings.Contains(keyStr, "/Table/13")  ||				//eventlog && rangelog
	//!strings.Contains(keyStr, "/Table/3/1") || !strings.Contains(keyStr, "/Table/2/1") ||	//descriptor && namespace
	strings.Contains(keyStr, "/Local/Range") || strings.Contains(keyStr, "Meta") || strings.Contains(keyStr, "System"))) {
		return true
	} else {
		return false
	}
}
func getObject(key MVCCKey) ([]byte, error){
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))

	keyStr := hex.EncodeToString([]byte(key.String2()))
	output, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(keyStr),
	})
	//fmt.Printf("Got Object : Key %s : %s : Value ", key.String(), keyStr)
	//check(err, "getObject ")
	if(err != nil) {
		//fmt.Println()
		return []byte("Error"), err
	} else {
		defer output.Body.Close()
		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, output.Body); err != nil {
			fmt.Println()
			return nil, err
		}
		//fmt.Printf(string(buf.Bytes()))
		//fmt.Println()
		return buf.Bytes(), err
	}
}

func deleteObject(key MVCCKey) string {
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))

	keyStr := hex.EncodeToString([]byte(key.String2()))
	output, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(keyStr),
	})
	fmt.Printf("Delete Object : Key %s\n", key.String2())
	check(err, "deleteObject ")
	return output.String()
}

func createObject(key MVCCKey, value []byte) string {
	if(len(value) == 0) {
		return deleteObject(key)
	}
	/*if !key.IsValue() {
		return ""
	}*/
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))

	keyStr := hex.EncodeToString([]byte(key.String2()))
	output, err := svc.PutObject(&s3.PutObjectInput{
		Body: strings.NewReader(string(value)),
		Bucket: aws.String(BUCKET),
		Key: aws.String(keyStr),
	})
	/*
	 * need to add / before every special character in key, for now doing ECS stuff only on user tables so no
	 * special characters are appearing in key.
	 */
	//fmt.Printf("Put Object : Key %s : %s : Value %s\n", key.String(), keyStr, string(value))
	fmt.Println("\nPut Object : Key %s : ", key.String2(), value)
	check(err, "putObject ")
	output2, _ := getObject(key)
	fmt.Println("ECS Value of this Put Key is ", output2)
	return output.String()
}

func check(e error, msg string) {
	if e != nil {
		fmt.Println("panic " + msg + e.Error())
		//panic(e)
	}
}
