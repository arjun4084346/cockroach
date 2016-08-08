// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Arjun Singh Bora (arjun4084346@gmail.com)

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

// Defining constants here.
// All KV pairs from all the databases go to the same storage,
// thus all the data can be stored in one bucket. However, multiple
// buckets can be created in future to share the data.
// KV_MAP is a Golang map, used as a in-mem primitive cache

var BUCKET string = "b1"
const ENDPOINT string = "http://10.247.78.217:9020"
var KV_MAP = map[string][]byte{}

// RocksDB->ECS change could not be tested on all the KV data.
// The reason being there is so much data and continuous queries to the data
// that it is increasing latency so much that SQL queries are getting timed out.
// KV_MAP is used to solve this problem, but it is not a stable caching mechanism,
// and still unable to handle all the data. Thus, we are not handling some system tables.
// qualifiedKey returns true if the key represents the tables we are handling data from.
// Tables we are skiping are : lease, UI, eventlog, rangelog, system, meta and local
func qualifiedKey(keyStr string) bool {
	if(!(strings.Contains(keyStr, "/Table/11") || strings.Contains(keyStr, "/Table/14")  ||			//lease && ui
			 strings.Contains(keyStr, "/Table/12") || strings.Contains(keyStr, "/Table/13")  ||				//eventlog && rangelog
			 strings.HasPrefix(keyStr, "/Local") || strings.HasPrefix(keyStr, "/Meta") || strings.HasPrefix(keyStr, "/System"))) {
		return true
	} else {
		return false
	}
}

// qualifiedIter returns true if iter is of type *engine.rocksDBIterator and
// engine.rocksDBIterator.replace is true, true signifies that the iterator holds
// the data fetched from ECS, and can be used by getECSKey() getECSValue() functions
func qualifiedIter(iter Iterator) bool {
	if iter.(*rocksDBIterator).replace && iter.(*rocksDBIterator).ECSvalid {	//ECSvalid not required coz replace is getting reset everyime needed
		return true
	}
	return false
}

// getObject returns the value of the key fetched from the ECS
// value is first searched in the KV_MAP, if not found in the map,
// fetched from the ECS and also stored in the KV_MAP before being returned
func getObject(key []byte) ([]byte, error){
	keyStr := hex.EncodeToString(key)
	data, present := KV_MAP[keyStr]
	if(present) {
		return data, nil
	}
	fmt.Printf("querying % x\n", key)		// actually it should never print, because everything is in KV_MAP
																			// which also means code below this line is not executing !!!
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))
	output, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(keyStr),
	})
	if(err != nil) {
		return nil, err
	} else {
		defer output.Body.Close()
		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, output.Body); err != nil {
			return nil, err
		}
		value := buf.Bytes()
		KV_MAP[keyStr] = value
		return value, err
	}
}

// deleteObject deletes the ECS object identified by mvcckey
func deleteObject(key []byte, mvcckey MVCCKey) error {
	fmt.Printf("DELETING %s\n", mvcckey)
	keyStr := hex.EncodeToString(key)
	delete(KV_MAP, keyStr)
	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))
	output, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(keyStr),
	})
	if(err != nil) {
		fmt.Println(output)
	}
	return err
}

// createObject stores a key-value pair into the ECS
// value is also stored in the KV_MAP
func createObject(key []byte, value []byte, mvcckey MVCCKey) error {
	keyStr := hex.EncodeToString(key)
	fmt.Printf("INSERTING %s\n", mvcckey)

	sess := session.New()
	svc := s3.New(sess, aws.NewConfig().WithRegion("us-west-2").WithEndpoint(ENDPOINT).WithS3ForcePathStyle(true))
	output, err := svc.PutObject(&s3.PutObjectInput{
		Body: strings.NewReader(string(value)),
		Bucket: aws.String(BUCKET),
		Key: aws.String(keyStr),
	})
	if(err == nil) {
		KV_MAP[keyStr] = value
	} else {
		fmt.Println(output)
	}

	return err
}

func check(e error, msg string) {
	if e != nil {
		fmt.Println("panic " + msg + e.Error())
		//panic(e)
	}
}
