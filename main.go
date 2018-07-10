package main

import (
	"fmt"
	neo4j "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/ONSdigital/go-ns/log"
	"os"
	"github.com/ONSdigital/dp-bolt/bolt"
	"errors"
	"strconv"
)

// Example of how to use library
func main() {
	pool, err := neo4j.NewClosableDriverPool("bolt://localhost:7687", 5)
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}
	db := bolt.New(pool)
	defer func() {
		log.Info("closing db connection pool", nil)
		db.Close()
	}()

	query := fmt.Sprintf("MATCH (cl:_code_list:`_name_%s`) WHERE cl.edition = %q RETURN count(*)", "mid-year-pop-age", "one-off")

	var count int64
	rowExtractor := func(r *bolt.Result) error {
		var ok bool
		count, ok = r.Data[0].(int64)
		if !ok {
			return errors.New("failed to cast result to int64")
		}
		return nil
	}

	err = db.QueryForResult(query, nil, rowExtractor)
	if err != nil {
		log.Error(err, nil)
		os.Exit(1)
	}

	if count != 1 {
		log.Error(errors.New("count in correct expected 1 but was "+strconv.FormatInt(count, 10)), nil)
		os.Exit(1)
	}
	log.Info("success", nil)
}
