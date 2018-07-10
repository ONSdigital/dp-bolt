package bolt

import (
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"io"
	"context"
)

type Row struct {
	Data  []interface{}
	Meta  map[string]interface{}
	Index int
}

type RowExtractorClosure func(result *Row) error

// DBPool contains the methods to control access to the Neo4J
// database pool
type DBPool interface {
	OpenPool() (bolt.Conn, error)
	Close() error
}

type DB struct {
	pool DBPool
}

//New create a new bolt.DB struct.
func New(pool DBPool) *DB {
	return &DB{pool: pool}
}

//Close attempts to close the db connection pool.
func (d *DB) Close() error {
	return d.pool.Close()
}

//QueryForResults executes the provided query to return 1 or more results.
func (d *DB) QueryForResults(ctx context.Context, cypherQuery string, params map[string]interface{}, resultExtractor RowExtractorClosure) error {
	return d.query(ctx, cypherQuery, params, resultExtractor, false)
}

//QueryForResults executes the provided query to return a single result.
func (d *DB) QueryForResult(ctx context.Context, cypherQuery string, params map[string]interface{}, resultExtractor RowExtractorClosure) error {
	return d.query(ctx, cypherQuery, params, resultExtractor, true)
}

func (d *DB) query(ctx context.Context, cypherQuery string, params map[string]interface{}, resultExtractor RowExtractorClosure, singleResult bool) error {
	conn, err := d.pool.OpenPool()
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "error opening neo4j connection"), nil)
		return err
	}
	defer conn.Close()

	rows, err := conn.QueryNeo(cypherQuery, params)
	if err != nil {
		return errors.WithMessage(err, "error executing neo4j query")
	}
	defer rows.Close()

	if err := d.extractResults(ctx, rows, resultExtractor, singleResult); err != nil {
		return errors.WithMessage(err, "error extracting row data")
	}

	return nil
}

func (d *DB) extractResults(ctx context.Context, rows bolt.Rows, resultExtractor RowExtractorClosure, singleResult bool) error {
	index := 0
	for {
		data, meta, err := rows.NextNeo()
		if err != nil {
			if err == io.EOF {
				log.InfoCtx(ctx, "extractResults: reached end of result rows", nil)
				return nil
			} else {
				log.ErrorCtx(ctx, errors.WithMessage(err, "row error, breaking loop"), nil)
				return err
			}
		}
		if singleResult && index > 0 {
			return errors.New("ExtractResult: expected single result but was not")
		}
		if err := resultExtractor(&Row{Data: data, Meta: meta, Index: index}); err != nil {
			return err
		}
		index++
	}
	return nil
}
