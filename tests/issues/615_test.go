package issues

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xiaoxiaolai/clickhouse-go"
	clickhouse_tests "github.com/xiaoxiaolai/clickhouse-go/tests"
)

func TestIssue615(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 0, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
	}
	require.NoError(t, err)
	if err := conn.Exec(
		context.Background(),
		`
			CREATE TABLE IF NOT EXISTS issue_615
			(id String, ts DateTime64(9))
			ENGINE = MergeTree
			ORDER BY (ts)
			`,
	); err != nil {
		require.NoError(t, err)
	}
	defer func() {
		require.NoError(t, conn.Exec(context.Background(), "DROP TABLE issue_615"))
	}()
	ts1 := time.Now().Round(time.Second)
	ts2 := ts1.Add(time.Millisecond)
	ts3 := ts1.Add(time.Second + time.Millisecond)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO issue_615 (id, ts)")
	require.NoError(t, err)
	require.NoError(t, batch.Append("first", ts1))
	require.NoError(t, batch.Append("second", ts2))
	require.NoError(t, batch.Append("third", ts3))
	require.NoError(t, batch.Send())
	rows, err := conn.Query(context.Background(), "SELECT id, ts from issue_615 where ts > @TS ORDER BY ts ASC", clickhouse.Named("TS", ts2))
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var (
			id string
			ts time.Time
		)
		require.NoError(t, rows.Scan(&id, &ts))
		i += 1
	}
	// loss of precision - should only get 1 result
	assert.Equal(t, 2, i)
	// use DateNamed to guarantee precision
	rows, err = conn.Query(context.Background(), "SELECT id, ts from issue_615 where ts > @TS ORDER BY ts ASC", clickhouse.DateNamed("TS", ts2, clickhouse.NanoSeconds))
	require.NoError(t, err)
	i = 0
	for rows.Next() {
		var (
			id string
			ts time.Time
		)
		require.NoError(t, rows.Scan(&id, &ts))
		require.Equal(t, id, "third")
		require.Equal(t, ts3.In(time.UTC), ts)
		i += 1
	}
	assert.Equal(t, 1, i)
	// test with timezone
	loc, _ := time.LoadLocation("Asia/Shanghai")
	rows, err = conn.Query(context.Background(), "SELECT id, ts from issue_615 where ts > @TS ORDER BY ts ASC", clickhouse.DateNamed("TS", ts2.In(loc), clickhouse.MilliSeconds))
	require.NoError(t, err)
	i = 0
	for rows.Next() {
		var (
			id string
			ts time.Time
		)
		require.NoError(t, rows.Scan(&id, &ts))
		require.Equal(t, ts3.In(time.UTC), ts)
		i += 1
	}
	assert.Equal(t, 1, i)
}
