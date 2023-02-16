package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clickhouse_tests "github.com/xiaoxiaolai/clickhouse-go/tests"

	"github.com/stretchr/testify/assert"
	"github.com/xiaoxiaolai/clickhouse-go"
)

func TestIssue504(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	require.NoError(t, err)
	var result []struct {
		Col1 string
		Col2 uint64
	}
	const query = `
		SELECT *
		FROM
		(
			SELECT
				'A'    AS Col1,
				number AS Col2
			FROM
			(
				SELECT number
				FROM system.numbers
				LIMIT 5
			)
		)
		WHERE (Col1, Col2) IN (@GS)
		`
	err = conn.Select(ctx, &result, query, clickhouse.Named("GS", []clickhouse.GroupSet{
		{Value: []interface{}{"A", 2}},
		{Value: []interface{}{"A", 4}},
	}))
	require.NoError(t, err)
	assert.Equal(t, []struct {
		Col1 string
		Col2 uint64
	}{
		{
			Col1: "A",
			Col2: 2,
		},
		{
			Col1: "A",
			Col2: 4,
		},
	}, result)
}
