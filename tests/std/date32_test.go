// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package std

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/xiaoxiaolai/clickhouse-go"
	clickhouse_tests "github.com/xiaoxiaolai/clickhouse-go/tests"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdDate32(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			if !CheckMinServerVersion(conn, 21, 9, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}
			const ddl = `
			CREATE TABLE test_date32 (
				  ID   UInt8
				, Col1 Date32
				, Col2 Nullable(Date32)
				, Col3 Array(Date32)
				, Col4 Array(Nullable(Date32))
				, Col5 Nullable(Date32)
				, Col6 Date32
			) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE test_date32")
			}()
			type result struct {
				ColID uint8 `ch:"ID"`
				Col1  time.Time
				Col2  *time.Time
				Col3  []time.Time
				Col4  []*time.Time
				Col5  sql.NullTime
				Col6  sql.NullTime
			}
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_date32")
			require.NoError(t, err)
			var (
				date1, _ = time.Parse("2006-01-02 15:04:05", "2283-11-11 00:00:00")
				date2, _ = time.Parse("2006-01-02 15:04:05", "1925-01-01 00:00:00")
			)
			_, err = batch.Exec(uint8(1), date1, &date2, []time.Time{date2}, []*time.Time{&date2, nil, &date1}, sql.NullTime{Time: time.Time{}, Valid: false}, sql.NullTime{Time: date1, Valid: true})
			require.NoError(t, err)
			_, err = batch.Exec(uint8(2), date2, nil, []time.Time{date1}, []*time.Time{nil, nil, &date2}, sql.NullTime{Time: time.Time{}, Valid: false}, sql.NullTime{Time: date2, Valid: true})
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				result1 result
				result2 result
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM test_date32 WHERE ID = $1", 1).Scan(
				&result1.ColID,
				&result1.Col1,
				&result1.Col2,
				&result1.Col3,
				&result1.Col4,
				&result1.Col5,
				&result1.Col6,
			))
			require.Equal(t, date1, result1.Col1)
			assert.Equal(t, "UTC", result1.Col1.Location().String())
			assert.Equal(t, date2, *result1.Col2)
			assert.Equal(t, []time.Time{date2}, result1.Col3)
			assert.Equal(t, []*time.Time{&date2, nil, &date1}, result1.Col4)
			assert.Equal(t, sql.NullTime{Time: time.Time{}, Valid: false}, result1.Col5)
			assert.Equal(t, sql.NullTime{Time: date1, Valid: true}, result1.Col6)
			require.NoError(t, conn.QueryRow("SELECT * FROM test_date32 WHERE ID = $1", 2).Scan(
				&result2.ColID,
				&result2.Col1,
				&result2.Col2,
				&result2.Col3,
				&result2.Col4,
				&result1.Col5,
				&result1.Col6,
			))
			require.Equal(t, date2, result2.Col1)
			require.Equal(t, "UTC", result2.Col1.Location().String())
			require.Nil(t, result2.Col2)
			assert.Equal(t, []time.Time{date1}, result2.Col3)
			assert.Equal(t, []*time.Time{nil, nil, &date2}, result2.Col4)
			assert.Equal(t, sql.NullTime{Time: time.Time{}, Valid: false}, result1.Col5)
			assert.Equal(t, sql.NullTime{Time: date2, Valid: true}, result1.Col6)
		})
	}
}
