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

func ECSIterPrev(SK MVCCKey, skip_current_key_versions bool) ECSIterState {
	newKey := make([]byte, len(SK.Key))
	newTimestamp := SK.Timestamp
	copy(newKey, SK.Key)

	if l := len(SK.Key); l == 0 {			// Why sometimes SK.Key is null!! -Arjun
		return ECSIterState{valid:false}
	}
	if skip_current_key_versions {
		newKey[len(newKey)-1] = newKey[len(newKey)-1] - 1
	} else {
		newTimestamp = newTimestamp.Next()
	}
	newSK := MVCCKey{
		Key 			: newKey,
		Timestamp	: newTimestamp,
	}
	return ECSIterSeekReverse(newSK, false, false)
}

func ECSIterNext(SK MVCCKey, skip_current_key_versions bool) ECSIterState {
	debug := false
	newKey := make([]byte, len(SK.Key))
	newTimestamp := SK.Timestamp
	copy(newKey, SK.Key)

	if l := len(SK.Key); l == 0 {
		return ECSIterState{valid:false}
	}
	if skip_current_key_versions {
		newKey[len(newKey)-1] = newKey[len(newKey)-1] + 1
	} else {
		newTimestamp = newTimestamp.Prev()
	}
	newSK := MVCCKey{
		Key 			: newKey,
		Timestamp	: newTimestamp,
	}
	return ECSIterSeek(newSK, false, debug, false)
}

func ECSIterSeekReverse(SK MVCCKey, prefix bool, reGetList bool) ECSIterState {
	if reGetList {
		getList(goToECSKey(SK), prefix)
	}
	for e := keyList.Back(); e != nil; e = e.Prev() {
		key := e.Value.(MVCCKey)
		if (prefix || SK.IsValue()) && !key.IsValue() {
			// It is observed that when prefix is true in the cockroach iterator or when search key
			// has timestamp, key without timestamp is not where iterator seeks to. Though I have not
			// read this anywhere in documentation. -Arjun
			// CHECK IF THIS RULE APPLICABLE IN REVERSE,NEXT,PREV AS WELL!! CANNOT, AS SEEKREVERSE DONT EVER EXECUTE !!
			continue
		}
		var c int
		if c = SK.Key.Compare(key.Key); c < 0 {
			continue
		}
		var keyChanged bool
		if c == 0 {
			keyChanged = false
		} else {
			keyChanged = true
		}
		var effectiveSKTS hlc.Timestamp
		if keyChanged {
			effectiveSKTS = hlc.ZeroTimestamp
		} else {
			effectiveSKTS = SK.Timestamp
			if SK.Timestamp == hlc.ZeroTimestamp {
				effectiveSKTS = hlc.MaxTimestamp
			}
		}
		oldkey := MVCCKey{Key:key.Key, Timestamp:key.Timestamp}
		maxDiffTS := hlc.MaxTimestamp
		var currDiff hlc.Timestamp
		for ; e != nil; e = e.Prev() {
			key = e.Value.(MVCCKey)
			if key.Key.Compare(oldkey.Key) != 0 {
				break
			}
			if keyChanged {
				currDiff = hlc.Timestamp{
					WallTime  :  key.Timestamp.WallTime - effectiveSKTS.WallTime,
					Logical   :  key.Timestamp.Logical - effectiveSKTS.Logical,
				}
				if currDiff.Less(maxDiffTS) {
					//we want min diff, lowest timestamp key
					maxDiffTS = currDiff
					oldkey = key
				}
			} else {
				if effectiveSKTS.Less(key.Timestamp) || effectiveSKTS.Equal(key.Timestamp) {
					currDiff = hlc.Timestamp{
						WallTime  : key.Timestamp.WallTime - effectiveSKTS.WallTime,
						Logical   : key.Timestamp.Logical - effectiveSKTS.Logical,
					}
					if currDiff.Less(maxDiffTS) {
						maxDiffTS = currDiff
						oldkey = key
					}
				}
			}
		}
		return ECSIterState{
			key:    oldkey,
			valid:  true,
		}
	}
	return ECSIterState{
		valid:	false,
	}
}

// This is the main replacement of C.DBIterSeek()
// SK 		- key being seeked
// prefix - when prefix is true, only keys with SK.key as a prefix are searched in ECS
// debug	- this is just for debugging purpose, can be safely removed
func ECSIterSeek(SK MVCCKey, prefix bool, debug bool, reGetList bool) ECSIterState {
	if reGetList {
		getList(goToECSKey(SK), prefix)
	}
	if debug {
		fmt.Println("seeked", SK)
	}
	for e := keyList.Front(); e != nil; e = e.Next() {
		key := e.Value.(MVCCKey)
		if (prefix || SK.IsValue()) && !key.IsValue() {		// It is observed that when prefix is true in the cockroach iterator or when search key
																											// has timestamp, key without timestamp is not where iterator seeks to. Though I have not
																											// read this anywhere in documentation. -Arjun
			continue
		}
		if debug {
			fmt.Println("comparing", SK, "to", key)
		}
		var c int
		if c = SK.Key.Compare(key.Key); c > 0 {
			continue
		}
		var keyChanged bool
		if c==0 {
			keyChanged = false
			if debug {
				fmt.Printf("keyChanged is %v now, SK was %s, while key caused this is %s\n", keyChanged, SK, key)
			}
		} else {
			keyChanged = true
			if debug {
				fmt.Printf("keyChanged is %v now, SK was %s, while key caused this is %s\n", keyChanged, SK, key)
			}
		}
		if false && keyChanged {
			if !key.IsValue() {
				if debug {
					fmt.Println("returning through new protocol", key)
				}
				return ECSIterState{
					key:		key,
					valid:	true,
				}
			}
		}
		var effectiveSKTS hlc.Timestamp
		if keyChanged {
			effectiveSKTS = hlc.ZeroTimestamp
		} else {
			effectiveSKTS = SK.Timestamp
			if SK.Timestamp==hlc.ZeroTimestamp {
				effectiveSKTS = hlc.MaxTimestamp
			}
		}
		oldkey := MVCCKey{Key:key.Key, Timestamp:key.Timestamp}
		if debug {
			fmt.Println("key set to", oldkey)
		}
		minDiffTS := hlc.ZeroTimestamp
		maxDiffTS := hlc.MaxTimestamp
		var currDiff hlc.Timestamp
		for ; e != nil; e = e.Next() {
			key = e.Value.(MVCCKey)
			if key.Key.Compare(oldkey.Key) != 0 {
				break
			}
			if debug {
				fmt.Println("comparing with", key)
			}
			if keyChanged {
				if debug {
					fmt.Println("considering1", key, SK.Timestamp.WallTime)
				}
				currDiff = hlc.Timestamp {
					WallTime	:	key.Timestamp.WallTime - effectiveSKTS.WallTime,
					Logical		:	key.Timestamp.Logical - effectiveSKTS.Logical,
				}
				if !currDiff.Less(minDiffTS) {	//we want max diff, biggest timestamp key
					minDiffTS = currDiff
					oldkey = key
					if debug {
						fmt.Println("accepted for now1", key)
					}
				}
			} else {
				if key.Timestamp.Less(effectiveSKTS) || effectiveSKTS.Equal(key.Timestamp){
						if debug {
							fmt.Println("considering2", key, SK.Timestamp.WallTime)
						}
						currDiff = hlc.Timestamp {
							WallTime	:	effectiveSKTS.WallTime-key.Timestamp.WallTime,
							Logical		:	effectiveSKTS.Logical-key.Timestamp.Logical,
						}
						if currDiff.Less(maxDiffTS) {
							maxDiffTS = currDiff
							oldkey = key
							if debug {
								fmt.Println("accepted for now2", key)
							}
						}
					}
			}
		}
		if debug {
			fmt.Println("returning", oldkey)
		}
		return ECSIterState{
			key:		oldkey,
			valid:	true,
		}
	}
	return ECSIterState{
		valid:	false,
	}
}

// getList fetches the key list from ECS ans store them in keyList
// prefixKey	- prefix to be search for
// prefix			- if prefix is true, prefix matching is done with prefixKey
func getList(prefixKey []byte, prefix bool ) {
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
		keyList.PushBack(ecsToGoKey(key))
	}
}
