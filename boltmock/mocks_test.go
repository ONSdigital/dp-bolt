package boltmock

import (
	"testing"
	"github.com/ONSdigital/dp-bolt/bolt"
	. "github.com/smartystreets/goconvey/convey"
	"errors"
)

func TestDB_QueryForResult(t *testing.T) {
	Convey("should...", t, func() {
		db := &DB{
			QueryForResultReturns: []error{nil, errors.New("whoop")},
		}

		params := map[string]interface{} {
			"key": int64(666),
		}

		err := db.QueryForResult("", params, nil)
		So(err, ShouldBeNil)
		So(db.QueryForResultCalls, ShouldHaveLength, 1)
		So(db.QueryForResultCalls[0].Query, ShouldEqual, "")
		So(db.QueryForResultCalls[0].Params, ShouldResemble, params)

		params["new"] = "aaaa"

		err = db.QueryForResult("123", params, nil)

		So(err, ShouldResemble, errors.New("whoop"))
		So(db.QueryForResultCalls, ShouldHaveLength, 2)
		So(db.QueryForResultCalls[1].Query, ShouldEqual, "123")
		So(db.QueryForResultCalls[1].Params, ShouldResemble, params)
	})
}

type BoltDB interface {
	QueryForResult(query string, params map[string]interface{}, resultExtractor bolt.ResultExtractor) error
}
