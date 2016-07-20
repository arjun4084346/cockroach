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
var KV_MAP = map[string][]byte{}

func qualifiedKey(keyStr string) bool {
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
	keyStr := hex.EncodeToString([]byte(key.String()))
	data, present := KV_MAP[keyStr]
	if(present) {
		return data, nil
	}
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))
	output, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(keyStr),
	})
	if(err != nil) {
		return []byte("Error"), err
	} else {
		defer output.Body.Close()
		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, output.Body); err != nil {
			fmt.Println("Error : Object parsing failed!!")
			return nil, err
		}
		value := buf.Bytes()
		KV_MAP[keyStr] = value
		return value, err
	}
}

func deleteObject(key MVCCKey) string {
	keyStr := hex.EncodeToString([]byte(key.String()))
	delete(KV_MAP, keyStr)
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))
	output, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(keyStr),
	})
	if(err != nil) {
		return "Error"
	}
	return output.String()
}

func createObject(key MVCCKey, value []byte) string {
	keyStr := hex.EncodeToString([]byte(key.String()))
	if(len(value) == 0) {		//Caution: This might be the wrong way to identify keys to remove. in case of secondary indexes, keys have NULL values.
													// need to check the difference
		return deleteObject(key)
	}
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))
	output, err := svc.PutObject(&s3.PutObjectInput{
		Body: strings.NewReader(string(value)),
		Bucket: aws.String(BUCKET),
		Key: aws.String(keyStr),
	})
	if(err != nil) {
		return "Error"
	}
	KV_MAP[keyStr] = value
	return output.String()
}

func check(e error, msg string) {
	if e != nil {
		fmt.Println("panic " + msg + e.Error())
		panic(e)
	}
}
