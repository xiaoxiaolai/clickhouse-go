package issues

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xiaoxiaolai/clickhouse-go"
	clickhouse_tests "github.com/xiaoxiaolai/clickhouse-go/tests"
	clickhouse_std_tests "github.com/xiaoxiaolai/clickhouse-go/tests/std"
)

func Test762(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	rows, err := conn.Query(context.Background(), "SELECT (NULL, NULL)")
	require.NoError(t, err)
	for rows.Next() {
		var (
			n []interface{}
		)
		require.NoError(t, rows.Scan(&n))
		require.Equal(t, []interface{}{(*interface{})(nil), (*interface{})(nil)}, n)
	}

}

func Test762Std(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	rows, err := conn.Query("SELECT tuple(NULL)")
	require.NoError(t, err)
	for rows.Next() {
		var (
			n interface{}
		)
		require.NoError(t, rows.Scan(&n))
		expected := []interface{}{(*interface{})(nil)}
		require.Equal(t, &expected, n)
	}
}
