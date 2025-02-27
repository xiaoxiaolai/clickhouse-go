package std

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/xiaoxiaolai/clickhouse-go"
	clickhouse_tests "github.com/xiaoxiaolai/clickhouse-go/tests/std"
	"time"
)

func UseContext() error {
	conn, err := GetStdOpenDBConnection(clickhouse.Native, nil, nil, nil)
	if err != nil {
		return err
	}
	if !clickhouse_tests.CheckMinServerVersion(conn, 22, 6, 1) {
		return nil
	}
	// we can use context to pass settings to a specific API call
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"allow_experimental_object_type": "1",
	}))
	conn.ExecContext(ctx, "DROP TABLE IF EXISTS example")
	// to create a JSON column we need allow_experimental_object_type=1
	if _, err = conn.ExecContext(ctx, `
		CREATE TABLE example (
				Col1 JSON
			) 
			Engine Memory
		`); err != nil {
		return err
	}

	// queries can be cancelled using the context
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		cancel()
	}()
	if err = conn.QueryRowContext(ctx, "SELECT sleep(3)").Scan(); err == nil {
		return fmt.Errorf("expected cancel")
	}

	// set a deadline for a query - this will cancel the query after the absolute time is reached. Again terminates the connection only,
	// queries will continue to completion in ClickHouse
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := conn.PingContext(ctx); err == nil {
		return fmt.Errorf("expected deadline exceeeded")
	}

	// set a query id to assist tracing queries in logs e.g. see system.query_log
	var one uint8
	ctx = clickhouse.Context(context.Background(), clickhouse.WithQueryID(uuid.NewString()))
	if err = conn.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return err
	}

	conn.ExecContext(context.Background(), "DROP QUOTA IF EXISTS foobar")
	defer func() {
		conn.ExecContext(context.Background(), "DROP QUOTA IF EXISTS foobar")
	}()
	ctx = clickhouse.Context(context.Background(), clickhouse.WithQuotaKey("abcde"))
	// set a quota key - first create the quota
	if _, err = conn.ExecContext(ctx, "CREATE QUOTA IF NOT EXISTS foobar KEYED BY client_key FOR INTERVAL 1 minute MAX queries = 5 TO default"); err != nil {
		return err
	}

	// queries can be cancelled using the context
	ctx, cancel = context.WithCancel(context.Background())
	// we will get some results before cancel
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": "1",
	}))
	rows, err := conn.QueryContext(ctx, "SELECT sleepEachRow(1), number FROM numbers(100);")
	if err != nil {
		return err
	}
	var (
		col1 uint8
		col2 uint8
	)

	for rows.Next() {
		if err := rows.Scan(&col1, &col2); err != nil {
			if col2 > 3 {
				fmt.Println("expected cancel")
				return nil
			}
			return err
		}
		fmt.Printf("row: col2=%d\n", col2)
		if col2 == 3 {
			cancel()
		}
	}
	return nil
}
