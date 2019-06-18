package oplog

import (
	"github.com/vinllen/mgo/bson"
	"reflect"
	"github.com/gugemichael/nimo4go"
)

const (
	PrimayKey = "_id"
)

type GenericOplog struct {
	Raw    []byte
	Parsed *PartialLog
}

type PartialLog struct {
	Timestamp     bson.MongoTimestamp `bson:"ts"`
	Operation     string              `bson:"op"`
	Gid           string              `bson:"g"`
	Namespace     string              `bson:"ns"`
	Object        bson.D              `bson:"o"`
	Query         bson.M              `bson:"o2"`
	UniqueIndexes bson.M              `bson:"uk"`
	Lsid          interface{}         `bson:"lsid"`        // mark the session id, used in transaction
	FromMigrate   bool                `bson:"fromMigrate"` // move chunk

	/*
	 * Every field subsequent declared is NEVER persistent or
	 * transfer on network connection. They only be parsed from
	 * respective logic
	 */
	UniqueIndexesUpdates bson.M // generate by CollisionMatrix
	RawSize              int    // generate by Decorator
	SourceId             int    // generate by Validator
}

func LogEntryEncode(logs []*GenericOplog) [][]byte {
	var encodedLogs [][]byte
	// log entry encode
	for _, log := range logs {
		encodedLogs = append(encodedLogs, log.Raw)
	}
	return encodedLogs
}

func LogParsed(logs []*GenericOplog) []*PartialLog {
	parsedLogs := make([]*PartialLog, len(logs), len(logs))
	for i, log := range logs {
		parsedLogs[i] = log.Parsed
	}
	return parsedLogs
}

func NewPartialLog(data bson.M) *PartialLog {
	partialLog := new(PartialLog)
	logType := reflect.TypeOf(*partialLog)
	for i:=0; i < logType.NumField(); i++ {
		tagName := logType.Field(i).Tag.Get("bson")
		if v, ok := data[tagName]; ok {
			reflect.ValueOf(partialLog).Elem().Field(i).Set(reflect.ValueOf(v))
		}
	}
	return partialLog
}

func (partialLog *PartialLog) Dump() bson.M {
	out := bson.M{}
	logType := reflect.TypeOf(*partialLog)
	for i:=0; i < logType.NumField(); i++ {
		if tagName, ok := logType.Field(i).Tag.Lookup("bson"); ok {
			out[tagName] = reflect.ValueOf(partialLog).Elem().Field(i).Interface()
		}
	}
	return out
}

func GetKey(log bson.D, wanted string) interface{} {
	if wanted == "" {
		wanted = PrimayKey
	}

	// "_id" is always the first field
	for _, ele := range log {
		if ele.Name == wanted {
			return ele.Value
		}
	}

	nimo.Assert("you can't see me")
	return nil
}

// convert bson.D to bson.M
func ConvertBsonD2M(input bson.D) bson.M {
	var m bson.M
	for _, ele := range input {
		m[ele.Name] = ele.Value
	}

	return m
}

func RemoveFiled(input bson.D, key string) bson.D {
	var id int
	for id = range input {
		if input[id].Name == key {
			break
		}
	}

	if id < len(input) {
		input = append(input[: id], input[id + 1:]...)
	}
	return input
}

func SetFiled(input bson.D, key string, value interface{}) {
	for _, ele := range input {
		if ele.Name == key {
			ele.Value = value
		}
	}
}