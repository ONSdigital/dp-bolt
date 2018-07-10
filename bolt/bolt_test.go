package bolt

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/ONSdigital/dp-bolt/bolt/mock"
	"github.com/pkg/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

var (
	closeNoErr = func() error {
		return nil
	}
	errTest = errors.New("dp-bolt error")
)

func TestDB_Close(t *testing.T) {

	Convey("Close should close connection pool", t, func() {
		pool := &mock.DBPoolMock{
			CloseFunc: closeNoErr,
		}
		db := New(pool)
		err := db.Close()

		So(err, ShouldBeNil)
		So(pool.CloseCalls(), ShouldHaveLength, 1)
	})
}

func TestDB_QueryForResultOpenConnErr(t *testing.T) {
	Convey("given pool.OpenPool returns an error", t, func() {
		pool := &mock.DBPoolMock{
			OpenPoolFunc: func() (golangNeo4jBoltDriver.Conn, error) {
				return nil, errTest
			},
		}
		db := New(pool)

		Convey("when QueryForResult is called", func() {
			var extractCalls []*Result
			mockExtractor := mockResultExtractor(extractCalls, nil)

			err := db.QueryForResult("", nil, mockExtractor)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "error opening neo4j connection").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
				So(extractCalls, ShouldHaveLength, 0)
			})
		})
	})
}

func TestDB_QueryForResult_QueryNeoError(t *testing.T) {
	Convey("given conn.QueryNeo return an error", t, func() {
		conn := &mock.NeoConnMock{
			QueryNeoFunc: func(query string, params map[string]interface{}) (golangNeo4jBoltDriver.Rows, error) {
				return nil, errTest
			},
			CloseFunc: closeNoErr,
		}
		pool := &mock.DBPoolMock{
			OpenPoolFunc: func() (golangNeo4jBoltDriver.Conn, error) {
				return conn, nil
			},
		}
		db := New(pool)

		Convey("when QueryForResult is called", func() {
			var extractCalls []*Result
			mockExtractor := mockResultExtractor(extractCalls, nil)

			err := db.QueryForResult("", nil, mockExtractor)

			Convey("then an error is returned and the connection is closed", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "error executing neo4j query").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)
				So(extractCalls, ShouldHaveLength, 0)
			})
		})
	})
}

func mockResultExtractor(extractCalls []*Result, err error) ResultExtractor {
	return func(r *Result) error {
		extractCalls = append(extractCalls, r)
		return err
	}
}
