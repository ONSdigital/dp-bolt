package bolt

import "github.com/pkg/errors"

type Update struct {
	RowsAffected int64
	LastInsertId int64
	Metadata     map[string]interface{}
}

type Stmt struct {
	Query  string
	Params map[string]interface{}
}

func (d *DB) Exec(s Stmt) (*Update, error) {
	conn, err := d.pool.OpenPool()
	if err != nil {
		return nil, errors.WithMessage(err, "error opening neo4j connection")
	}
	defer conn.Close()

	stmt, err := conn.PrepareNeo(s.Query)
	if err != nil {
		return nil, errors.WithMessage(err, "error creating no4j statement")
	}
	defer stmt.Close()

	res, err := stmt.ExecNeo(s.Params)
	if err != nil {
		return nil, errors.WithMessage(err, "error executing statement")
	}

	rowsAffected, _ := res.RowsAffected()
	lastInsertID, _ := res.LastInsertId()
	meta := res.Metadata()

	return &Update{
		RowsAffected: rowsAffected,
		LastInsertId: lastInsertID,
		Metadata:     meta,
	}, nil
}
