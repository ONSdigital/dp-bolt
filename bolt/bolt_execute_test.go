package bolt

import (
	"testing"
	"github.com/ONSdigital/dp-bolt/bolt/mock"
	neo4j "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/pkg/errors"
)

var (
	Err  = errors.New("error")
	stmt = Stmt{
		Query:  "123",
		Params: map[string]interface{}{"key": "value"},
	}
)

func TestDB_ExecSuccess(t *testing.T) {
	Convey("should return expected result for successfully executed statement", t, func() {
		res := &mock.NeoResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return int64(1), nil
			},
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{"key": "value"}
			},
			LastInsertIdFunc: func() (int64, error) {
				return 0, nil
			},
		}

		conn := &mock.NeoConnMock{
			CloseFunc: closeNoErr,
			ExecNeoFunc: func(query string, params map[string]interface{}) (neo4j.Result, error) {
				return res, nil
			},
		}

		pool := &mock.DBPoolMock{
			CloseFunc: closeNoErr,
			OpenPoolFunc: func() (neo4j.Conn, error) {
				return conn, nil
			},
		}

		db := DB{pool: pool}
		rowsAffected, meta, err := db.Exec(stmt)

		So(conn.CloseCalls(), ShouldHaveLength, 1)
		So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
		So(conn.ExecNeoCalls(), ShouldHaveLength, 1)
		So(conn.ExecNeoCalls()[0].Query, ShouldEqual, stmt.Query)
		So(conn.ExecNeoCalls()[0].Params, ShouldResemble, stmt.Params)
		So(rowsAffected, ShouldEqual, int64(1))
		So(meta, ShouldResemble, map[string]interface{}{"key": "value"})
		So(err, ShouldBeNil)
	})
}

func TestDB_ExecNoQuery(t *testing.T) {
	Convey("should return expected response if an empty Stmt is provided", t, func() {
		res := &mock.NeoResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return int64(1), nil
			},
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{"key": "value"}
			},
			LastInsertIdFunc: func() (int64, error) {
				return 0, nil
			},
		}

		conn := &mock.NeoConnMock{
			CloseFunc: closeNoErr,
			ExecNeoFunc: func(query string, params map[string]interface{}) (neo4j.Result, error) {
				return res, nil
			},
		}

		pool := &mock.DBPoolMock{
			CloseFunc: closeNoErr,
			OpenPoolFunc: func() (neo4j.Conn, error) {
				return conn, nil
			},
		}

		db := DB{pool: pool}

		rowsAffected, meta, err := db.Exec(Stmt{})

		So(conn.CloseCalls(), ShouldHaveLength, 0)
		So(pool.OpenPoolCalls(), ShouldHaveLength, 0)
		So(conn.ExecNeoCalls(), ShouldHaveLength, 0)
		So(rowsAffected, ShouldEqual, int64(0))
		So(meta, ShouldBeNil)
		So(err, ShouldBeNil)
	})
}

func TestDB_ExecOpenConnError(t *testing.T) {
	Convey("should return expected error if pool.OpenPool returns an error", t, func() {
		pool := &mock.DBPoolMock{
			CloseFunc: closeNoErr,
			OpenPoolFunc: func() (neo4j.Conn, error) {
				return nil, Err
			},
		}

		db := DB{pool: pool}

		rowsAffected, meta, err := db.Exec(stmt)

		So(err, ShouldResemble, errors.WithMessage(Err, "error opening neo4j connection"))
		So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
		So(rowsAffected, ShouldEqual, int64(0))
		So(meta, ShouldBeNil)
	})
}

func TestDB_ExecExecNeoError(t *testing.T) {
	Convey("should return expected error if conn.ExecNeo returns an error", t, func() {
		conn := &mock.NeoConnMock{
			CloseFunc: closeNoErr,
			ExecNeoFunc: func(query string, params map[string]interface{}) (neo4j.Result, error) {
				return nil, Err
			},
		}

		pool := &mock.DBPoolMock{
			CloseFunc: closeNoErr,
			OpenPoolFunc: func() (neo4j.Conn, error) {
				return conn, nil
			},
		}

		db := DB{pool: pool}

		rowsAffected, meta, err := db.Exec(stmt)

		So(conn.CloseCalls(), ShouldHaveLength, 1)
		So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
		So(conn.ExecNeoCalls(), ShouldHaveLength, 1)
		So(conn.ExecNeoCalls()[0].Query, ShouldEqual, stmt.Query)
		So(conn.ExecNeoCalls()[0].Params, ShouldResemble, stmt.Params)
		So(rowsAffected, ShouldEqual, int64(0))
		So(meta, ShouldBeNil)
		So(err, ShouldResemble, errors.WithMessage(Err, "error executing statement"))
	})
}

func TestDB_ExecResultRowsAffectedError(t *testing.T) {
	Convey("should return expected result if result.RowsAffected returns an error", t, func() {
		res := &mock.NeoResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 0, Err
			},
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{"key": "value"}
			},
			LastInsertIdFunc: func() (int64, error) {
				return 0, nil
			},
		}

		conn := &mock.NeoConnMock{
			CloseFunc: closeNoErr,
			ExecNeoFunc: func(query string, params map[string]interface{}) (neo4j.Result, error) {
				return res, nil
			},
		}

		pool := &mock.DBPoolMock{
			CloseFunc: closeNoErr,
			OpenPoolFunc: func() (neo4j.Conn, error) {
				return conn, nil
			},
		}

		db := DB{pool: pool}
		rowsAffected, meta, err := db.Exec(stmt)

		So(conn.CloseCalls(), ShouldHaveLength, 1)
		So(pool.OpenPoolCalls(), ShouldHaveLength, 1)
		So(conn.ExecNeoCalls(), ShouldHaveLength, 1)
		So(conn.ExecNeoCalls()[0].Query, ShouldEqual, stmt.Query)
		So(conn.ExecNeoCalls()[0].Params, ShouldResemble, stmt.Params)
		So(rowsAffected, ShouldEqual, int64(0))
		So(meta, ShouldBeNil)
		So(err, ShouldResemble, errors.WithMessage(Err, "error getting rows affected count from result"))
	})
}
