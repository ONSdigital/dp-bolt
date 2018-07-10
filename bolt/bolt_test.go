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
			closure := func(r *Result) error {
				extractCalls = append(extractCalls, r)
				return nil
			}

			err := db.QueryForResult("", nil, closure)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "error opening neo4j connection").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
				So(extractCalls, ShouldHaveLength, 0)
			})
		})
	})
}
