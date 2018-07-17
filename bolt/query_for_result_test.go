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

type ResultMapperMock struct {
	Calls         []Result
	MapResultFunc ResultMapper
}

func (m *ResultMapperMock) Do(r *Result) error {
	m.Calls = append(m.Calls, *r)
	return m.MapResultFunc(r)
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
			rowMapper := ResultMapperMock{
				Calls: []Result{},
				MapResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, rowMapper.Do)

			Convey("then an error is returned and result count is 0", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "error opening neo4j connection").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
				So(rowMapper.Calls, ShouldHaveLength, 0)
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
			resultMappper := ResultMapperMock{
				Calls: []Result{},
				MapResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, resultMappper.Do)

			Convey("then an error is returned and the connection is closed", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "error executing neo4j query").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)
				So(resultMappper.Calls, ShouldHaveLength, 0)
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
			resultMapper := ResultMapperMock{
				Calls: []Result{},
				MapResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, resultMapper.Do)

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

				So(resultMapper.Calls, ShouldHaveLength, 0)
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
			resultMapper := ResultMapperMock{
				Calls: []Result{},
				MapResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, resultMapper.Do)

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

				So(resultMapper.Calls, ShouldHaveLength, 1)
				So(resultMapper.Calls[0].Data, ShouldResemble, expectedData)
				So(resultMapper.Calls[0].Meta, ShouldResemble, expectedMeta)
			})
		})
	})
}

func TestDB_QueryForResult_ExtractResultError(t *testing.T) {
	Convey("given ResultMapper returns an error", t, func() {
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

			resultMapper := ResultMapperMock{
				Calls: []Result{},
				MapResultFunc: func(r *Result) error {
					return errTest
				},
			}

			err := db.QueryForResult("", nil, resultMapper.Do)

			Convey("then an error is returned and the connection and rows are closed", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, errors.WithMessage(errTest, "mapResult returned an error").Error())
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)

				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)

				So(rows.NextNeoCalls(), ShouldHaveLength, 1)
				So(rows.CloseCalls(), ShouldHaveLength, 1)

				So(resultMapper.Calls, ShouldHaveLength, 1)
				So(resultMapper.Calls[0].Data, ShouldResemble, expectedData)
				So(resultMapper.Calls[0].Meta, ShouldResemble, expectedMeta)
				So(resultMapper.Calls[0].Index, ShouldEqual, 0)
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

			resultMapper := ResultMapperMock{
				Calls: []Result{},
				MapResultFunc: func(r *Result) error {
					return nil
				},
			}

			err := db.QueryForResult("", nil, resultMapper.Do)

			Convey("then no error is returned and the connection and rows are closed", func() {
				So(err, ShouldBeNil)
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)

				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)

				So(rows.NextNeoCalls(), ShouldHaveLength, 2)
				So(rows.CloseCalls(), ShouldHaveLength, 1)

				So(resultMapper.Calls, ShouldHaveLength, 1)
				So(resultMapper.Calls[0].Data, ShouldResemble, expectedData)
				So(resultMapper.Calls[0].Meta, ShouldResemble, expectedMeta)
				So(resultMapper.Calls[0].Index, ShouldEqual, 0)
			})
		})
	})
}

func TestDB_QueryForResult_NoResults(t *testing.T) {
	Convey("given a no results are returned", t, func() {
		rowsStubs := &mock.RowsStub{
			Rows: []mock.RowValues{
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

			resultMapper := ResultMapperMock{
				Calls: []Result{},
			}

			err := db.QueryForResult("", nil, resultMapper.Do)

			Convey("then no results error is returned", func() {
				So(err, ShouldEqual, ErrNoResults)
				So(pool.OpenPoolCalls(), ShouldHaveLength, 1)

				So(conn.QueryNeoCalls(), ShouldHaveLength, 1)
				So(conn.QueryNeoCalls()[0].Query, ShouldEqual, "")
				So(conn.QueryNeoCalls()[0].Params, ShouldEqual, nil)
				So(conn.CloseCalls(), ShouldHaveLength, 1)

				So(rows.NextNeoCalls(), ShouldHaveLength, 1)
				So(rows.CloseCalls(), ShouldHaveLength, 1)

				So(resultMapper.Calls, ShouldHaveLength, 0)
			})
		})
	})
}
