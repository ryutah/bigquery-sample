package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

var (
	project = os.Getenv("PROJECT")
	table   = os.Getenv("TABLE")
)

var query = fmt.Sprintf("SELECT * FROM `%s` LIMIT 1000", table)

type value struct {
	URL   string              `bigquery:"url"`
	Label bigquery.NullString `bigquery:"label"`
}

func main() {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("couldn't create client: %v", err)
	}
	defer client.Close()

	dst := make([]*value, 0)
	if err := find(ctx, client, query, &dst); err != nil {
		log.Fatal(err)
	}

	for _, v := range dst {
		fmt.Println(v)
	}
}

func find(ctx context.Context, client *bigquery.Client, query string, dst interface{}) error {
	dt := reflect.TypeOf(dst)
	if dt.Kind() != reflect.Ptr {
		return fmt.Errorf("parmeter 'dst' must be slice of ptr")
	}
	if dt.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("parmeter 'dst' must be slice of ptr")
	}

	dstSliceVal := reflect.ValueOf(dst).Elem()

	sliceValType := dt.Elem().Elem()
	newValType := sliceValType
	if sliceValType.Kind() == reflect.Ptr {
		newValType = sliceValType.Elem()
	}

	q := client.Query(query)
	q.UseStandardSQL = true
	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	for {
		ret := reflect.New(newValType)
		if err := it.Next(ret.Interface()); err == iterator.Done {
			break
		} else if err != nil {
			return fmt.Errorf("failed to load query result: %v", err)
		}
		if sliceValType.Kind() == reflect.Ptr {
			dstSliceVal.Set(reflect.Append(dstSliceVal, ret))
		} else {
			dstSliceVal.Set(reflect.Append(dstSliceVal, ret.Elem()))
		}
	}

	return nil
}
