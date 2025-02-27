package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xiaoxiaolai/clickhouse-go"
	clickhouse_tests "github.com/xiaoxiaolai/clickhouse-go/tests"
)

func TestIssue506(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)

	const ddlA = `
		CREATE TABLE test_append_struct_a (
			  Col1  UInt32
			, Col2  String
			, Col3  Array(String)
			, Col4  Nullable(UInt8)
		) Engine MergeTree() ORDER BY tuple()
		`

	const ddlB = `
		CREATE TABLE test_append_struct_b (
			  Col4  Array(UInt32)
			, Col3  Nullable(UInt8)
			, Col2  UInt32
			, Col1  String
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_append_struct_a")
		conn.Exec(ctx, "DROP TABLE test_append_struct_b")
	}()

	numQueries := 10
	numRows := 10000
	rowsPerQuery := numRows / numQueries
	ch := make(chan bool, numQueries)

	assert.NoError(t, conn.Exec(ctx, ddlA))
	assert.NoError(t, conn.Exec(ctx, ddlB))
	assert.NoError(t, conn.Exec(ctx, `INSERT INTO test_append_struct_a SELECT number, concat('Str_', toString(number)), 
											[concat('Str_', toString(number)), '', concat('Str_', toString(number))], NULL FROM system.numbers 
											LIMIT 10000;`))

	assert.NoError(t, conn.Exec(ctx, `INSERT INTO test_append_struct_b SELECT [number, number + 1, number + 2], 
											NULL, number, concat('Str_', toString(number)) FROM system.numbers LIMIT 10000;`))

	type dataA struct {
		Col1 uint32
		Col2 string
		Col3 []string
		Col4 *uint8
	}

	type dataB struct {
		Col4 []uint32
		Col3 *uint8
		Col2 uint32
		Col1 string
	}

	for i := 0; i < numQueries; i++ {
		go func(qNum int) {
			l := rowsPerQuery * qNum
			u := rowsPerQuery * (qNum + 1)
			r := l

			var query string
			if qNum%2 == 1 {
				var results []dataB
				query = fmt.Sprintf("SELECT * FROM test_append_struct_b WHERE Col2 >= %d and Col2 < %d ORDER BY Col2 ASC", l, u)
				if err := conn.Select(ctx, &results, query); assert.NoError(t, err) {
					for _, result := range results {
						str := fmt.Sprintf("Str_%d", r)
						assert.Equal(t, dataB{
							Col4: []uint32{uint32(r), uint32(r) + 1, uint32(r) + 2},
							Col3: nil,
							Col2: uint32(r),
							Col1: str,
						}, result)
						r++
					}
				}
			} else {
				var results []dataA
				query := fmt.Sprintf("SELECT * FROM test_append_struct_a WHERE Col1 >= %d and Col1 < %d ORDER BY Col1 ASC", l, u)
				if err := conn.Select(ctx, &results, query); assert.NoError(t, err) {
					for _, result := range results {
						str := fmt.Sprintf("Str_%d", r)
						assert.Equal(t, dataA{
							Col1: uint32(r),
							Col2: str,
							Col3: []string{str, "", str},
							Col4: nil,
						}, result)
						r++
					}
				}
			}
			ch <- true
		}(i)
	}

	for numQueries > 0 {
		finished := <-ch
		assert.True(t, finished)
		numQueries--
	}

}
