package issues

import (
	"context"
	"testing"
	"time"

	clickhouse_tests "github.com/xiaoxiaolai/clickhouse-go/tests"

	"github.com/stretchr/testify/assert"
	"github.com/xiaoxiaolai/clickhouse-go"
)

func TestIssue548(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)

	assert.NoError(t, err)
	// give it plenty of time before we conclusively assume deadlock
	timeout := time.After(5 * time.Second)
	done := make(chan bool)
	go func() {
		// should take 1s
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rows, _ := conn.Query(ctx, "SELECT sleepEachRow(0.001) as Col1 FROM system.numbers LIMIT 1000 SETTINGS max_block_size=10;")
		rows.Close()
		done <- true
	}()

	select {
	case <-timeout:
		t.Fatal("Close() deadlocked")
	case <-done:
	}
}
