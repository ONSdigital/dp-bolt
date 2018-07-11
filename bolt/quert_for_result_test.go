package bolt

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/ONSdigital/dp-bolt/bolt/mock"
	"github.com/pkg/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"io"
)

var (
	closeNoErr = func() error {
		return nil
	}

	errTest = errors.New("dp-bolt error")

	expectedData = []interface{}{int64(1)}
	expectedMeta = map[string]interface{}{"key": "value"}
)

type queryParams struct {
	data  []interface{}
	meta  map[string]interface{}
	index int
}

type ResultExtractorMock struct {
	Calls             []Result
	ExtractResultFunc ResultExtractor
}

func (m *ResultExtractorMock) ExtractResult(r *Result) error {
	m.Calls = append(m.Calls, *r)
	return m.ExtractResultFunc(r)
}

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
			mockExtractor := ResultExtractorMock{
				Calls: []Result{},
				ExtractResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, mockExtractor.ExtractResult)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "error opening neo4j connection").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
				So(mockExtractor.Calls, ShouldHaveLength, 0)
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
			mockExtractor := ResultExtractorMock{
				Calls: []Result{},
				ExtractResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, mockExtractor.ExtractResult)

			Convey("then an error is returned and the connection is closed", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "error executing neo4j query").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)
				So(mockExtractor.Calls, ShouldHaveLength, 0)
			})
		})
	})
}

func TestDB_QueryForResult_NextNeoError(t *testing.T) {
	Convey("given row.NextNeo returns an error", t, func() {

		rows := &mock.NeoRowsMock{
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return nil, nil, errTest
			},
			CloseFunc: closeNoErr,
		}

		conn := &mock.NeoConnMock{
			QueryNeoFunc: func(query string, params map[string]interface{}) (golangNeo4jBoltDriver.Rows, error) {
				return rows, nil
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
			mockExtractor := ResultExtractorMock{
				Calls: []Result{},
				ExtractResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, mockExtractor.ExtractResult)

			Convey("then an error is returned and the connection and rows are closed", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "extractResults: rows.NextNeo() return unexpected error").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)

				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)

				So(rows.NextNeoCalls(), ShouldHaveLength, 1)
				So(rows.CloseCalls(), ShouldHaveLength, 1)

				So(mockExtractor.Calls, ShouldHaveLength, 0)
			})
		})
	})
}

func TestDB_QueryForResult_MoreThanOneResult(t *testing.T) {
	Convey("given row.NextNeo returns more than 1 result row", t, func() {
		results := []*Result{
			{
				Data:  expectedData,
				Meta:  expectedMeta,
				Index: 0,
			},
			{
				Data:  expectedData,
				Meta:  expectedMeta,
				Index: 1,
			},
		}

		i := 0
		rows := &mock.NeoRowsMock{
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				defer func() { i ++ }()
				return results[i].Data, results[i].Meta, nil
			},
			CloseFunc: closeNoErr,
		}

		conn := &mock.NeoConnMock{
			QueryNeoFunc: func(query string, params map[string]interface{}) (golangNeo4jBoltDriver.Rows, error) {
				return rows, nil
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
			mockExtractor := ResultExtractorMock{
				Calls: []Result{},
				ExtractResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, mockExtractor.ExtractResult)

			Convey("then an error is returned and the connection and rows are closed", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, NonUniqueResult.Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)

				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)

				So(rows.NextNeoCalls(), ShouldHaveLength, 2)
				So(rows.CloseCalls(), ShouldHaveLength, 1)

				So(mockExtractor.Calls, ShouldHaveLength, 1)
				So(mockExtractor.Calls[0].Data, ShouldResemble, expectedData)
				So(mockExtractor.Calls[0].Meta, ShouldResemble, expectedMeta)
			})
		})
	})
}

func TestDB_QueryForResult_ExtractResultError(t *testing.T) {
	Convey("given ResultExtractor returns an error", t, func() {
		expectedData := []interface{}{int64(1)}
		expectedMeta := map[string]interface{}{"key": "value"}

		rowsStubs := &mock.RowsStub{
			Rows: []mock.RowValues{
				{
					Data: expectedData,
					Meta: expectedMeta,
					Err:  nil,
				},
				{
					Data: nil,
					Meta: nil,
					Err:  io.EOF,
				},
			},
		}

		rows := &mock.NeoRowsMock{
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return rowsStubs.Next()
			},
			CloseFunc: closeNoErr,
		}

		conn := &mock.NeoConnMock{
			QueryNeoFunc: func(query string, params map[string]interface{}) (golangNeo4jBoltDriver.Rows, error) {
				return rows, nil
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

			mockExtractor := ResultExtractorMock{
				Calls: []Result{},
				ExtractResultFunc: func(r *Result) error {
					return errTest
				},
			}

			err := db.QueryForResult("", nil, mockExtractor.ExtractResult)

			Convey("then an error is returned and the connection and rows are closed", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "extractResults: extractResult returned an error").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)

				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)

				So(rows.NextNeoCalls(), ShouldHaveLength, 1)
				So(rows.CloseCalls(), ShouldHaveLength, 1)

				So(mockExtractor.Calls, ShouldHaveLength, 1)
				So(mockExtractor.Calls[0].Data, ShouldResemble, expectedData)
				So(mockExtractor.Calls[0].Meta, ShouldResemble, expectedMeta)
				So(mockExtractor.Calls[0].Index, ShouldEqual, 0)
			})
		})
	})
}

func TestDB_QueryForResult_Success(t *testing.T) {
	Convey("given a single result is returned", t, func() {
		expectedData := []interface{}{int64(1)}
		expectedMeta := map[string]interface{}{"key": "value"}

		rowsStubs := &mock.RowsStub{
			Rows: []mock.RowValues{
				{
					Data: expectedData,
					Meta: expectedMeta,
					Err:  nil,
				},
				{
					Data: nil,
					Meta: nil,
					Err:  io.EOF,
				},
			},
		}

		rows := &mock.NeoRowsMock{
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return rowsStubs.Next()
			},
			CloseFunc: closeNoErr,
		}

		conn := &mock.NeoConnMock{
			QueryNeoFunc: func(query string, params map[string]interface{}) (golangNeo4jBoltDriver.Rows, error) {
				return rows, nil
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

			mockExtractor := ResultExtractorMock{
				Calls: []Result{},
				ExtractResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, mockExtractor.ExtractResult)

			Convey("then no error is returned and the connection and rows are closed", func() {
				So(err, ShouldBeNil)
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)

				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)

				So(rows.NextNeoCalls(), ShouldHaveLength, 2)
				So(rows.CloseCalls(), ShouldHaveLength, 1)

				So(mockExtractor.Calls, ShouldHaveLength, 1)
				So(mockExtractor.Calls[0].Data, ShouldResemble, expectedData)
				So(mockExtractor.Calls[0].Meta, ShouldResemble, expectedMeta)
				So(mockExtractor.Calls[0].Index, ShouldEqual, 0)
			})
		})
	})
}

