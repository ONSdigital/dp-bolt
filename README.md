# dp-bolt
Library adding a layer of abstraction around the low level Neo4j bolt driver handling the boilerplate Neo4j query code
 for you. Simply provide a query, parameters and a closure to extract the row data.
 
## Getting started
`go get github.com/ONSdigital/dp-bolt`

### Creating a bolt.DB
```go
pool, err := neo4j.NewClosableDriverPool("$bolt_url$", 1)
if err != nil {
	// handle error
}

db := bolt.New(pool)
defer db.Close()
```

### ResultExtractorClosure
The final parameter of `QueryForResult()` is a `ResultExtractor` - a closure that enables you to customize how to 
handle the row data return by your query - the underlying library returns row data as `[]interface{}` - the closure you provide
should handle casting the data to the expected type and assigning to a variable for later use.
```go
var count int64
rowExtractor := func(r *bolt.Result) error {
    var ok bool
    count, ok = r.Data[0].(int64)
    if !ok {
        return errors.New("failed to cast result to int64")
    }
    return nil
}
```

### Query for a single result
```go
var count int64
rowExtractor := func(r *bolt.Result) error {
    var ok bool
    count, ok = r.Data[0].(int64)
    if !ok {
        return errors.New("failed to cast result to int64")
    }
    return nil
}

err = db.QueryForResult("MATCH (n) RETURN count(*)", nil, rowExtractor)
if err != nil {
    // handle error
}
// do something with count ...
```

