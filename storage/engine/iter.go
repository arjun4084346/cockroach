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
	"encoding/hex"
	"container/list"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// keyList holds list of keys fetched from the ECS in lexicographic order
// keyListCommonKey holds list of keys with common user-key in lexicographic order
var keyList = list.New()
var keyListCommonKey = list.New()

// state of ECS Iterator, valid is true when it holds a valid key
type ECSIterState struct {
	key		MVCCKey;
	valid	bool
}

// This is the main replacement of C.DBIterSeek()
// SK 		- key being seeked
// prefix - when prefix is true, only keys with SK.key as a prefix are searched in ECS
// debug	- this is just for debugging purpose, can be safely removed
func ECSIterSeek(SK MVCCKey, prefix bool, debug bool) ECSIterState {
	getList(goToECSKey(SK), prefix, SK, debug)
	for e := keyList.Front(); e != nil; e = e.Next() {
		key := e.Value.(MVCCKey)
		if (prefix || SK.IsValue()) && !key.IsValue() {		// It is observed that when prefix is true in the cockroach iterator or when search key
																											// has timestamp, key without timestamp is not where iterator seeks to. Though I have not
																											// read this anywhere in documentation. -Arjun
			fmt.Println("skipping", key)
			continue
		}
		if debug {
			fmt.Println("comparing", SK, "to", key)
		}
		if key.Key.StartsWith(SK.Key) {		// uses secondaryECSIterSeek() to find the correct timestamp'ed key for a particular user-key
			secondaryECSIterState := secondaryECSIterSeek(key, SK.Timestamp, debug)
			if !secondaryECSIterState.valid {
				continue
			}
			return secondaryECSIterState
		}
		if c := SK.Key.Compare(key.Key); c < 0 {		// c == 0 should have been covered in case above
			return ECSIterState{											// c < 0 i.e. SK < key
				key:		key,
				valid:	true,
			}
		}
	}
	return ECSIterState{
		valid:	false,
	}
}

// timestamped are sorted in reverse order in ECS. secondaryECSIterSeek search for the correct key in reverse order
// prefixKey	- MVCC Key used for prefix matching; actually only the MVCC.key is needed, but complete key is passed for debugging
// seekKeyTS	- time stamp of the MVCC key originally seeked for
// debug			- this is just for debugging purpose, can be safely removed
func secondaryECSIterSeek(prefixKey MVCCKey, seekKeyTS hlc.Timestamp, debug bool) ECSIterState {
	newSK := MakeMVCCMetadataKey(prefixKey.Key)
	if debug {
		fmt.Println("Sending new Key", newSK)
	}
	getListCommonKey(goToECSKey(newSK), true, prefixKey, debug)
	for e := keyListCommonKey.Back(); e != nil; e = e.Prev() {
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
				valid:	true,
			}
		}
	}
	return ECSIterState{
		valid:	false,
	}
}

// getListCommonKey is same as getList
func getListCommonKey(prefixKey []byte, prefix bool, SK MVCCKey, debug bool) {
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
	s3Contents := output.Contents
	keyListCommonKey.Init()
	for _, s3Content := range s3Contents {
		key, _ := hex.DecodeString(*s3Content.Key)
		var mvccKey MVCCKey
		if(len(key) > 12) {
			keybuf := key[:len(key)-12]
			ts := getTimestamp(key[len(key)-12:])
			mvccKey = MVCCKey{
				Key:       keybuf,
				Timestamp : ts,
			}
		} else {
			mvccKey = MVCCKey{
				Key:       key,
			}
		}
		keyListCommonKey.PushBack(mvccKey)
	}
	if(debug) {
		fmt.Printf("Secondary : %d keys, key %s % x\n", keyListCommonKey.Len(), SK, prefixKey)
	}
}

// getList fetches the key list from ECS ans store them in keyList
// prefixKey	- prefix to be search for
// prefix			- if prefix is true, prefix matching is done with prefixKey
// SK, debug	- only for debugging purposes
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
	s3Contents := output.Contents
	keyList.Init()
	for _, s3Content := range s3Contents {
		key, _ := hex.DecodeString(*s3Content.Key)
		var mvccKey MVCCKey
		l := len(key)
		if(l > 12) {
			keybuf := key[:l-12]
			ts := getTimestamp(key[l-12:])
			mvccKey = MVCCKey{
				Key:       keybuf,
				Timestamp : ts,
			}
		} else {
			mvccKey = MVCCKey{
				Key:       key,
			}
		}
		keyList.PushBack(mvccKey)
	}
	if(debug) {
		fmt.Printf("%d keys, prefix %v, key %s % x\n", keyList.Len(), prefix, SK, prefixKey)
	}
}
