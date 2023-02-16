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

func Test783(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"flatten_nested": 1,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	row := conn.QueryRow(ctx, "SELECT groupArray(('a', ['time1', 'time2'])) as val")
	var x [][]interface{}
	require.NoError(t, row.Scan(&x))
	require.Equal(t, [][]interface{}{{"a", []string{"time1", "time2"}}}, x)
}

func TestStd783(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	require.NoError(t, err)
	row := conn.QueryRow("SELECT groupArray(('a', ['time1', 'time2'])) as val")
	var x [][]interface{}
	require.NoError(t, row.Scan(&x))
	require.Equal(t, [][]interface{}{{"a", []string{"time1", "time2"}}}, x)
}
