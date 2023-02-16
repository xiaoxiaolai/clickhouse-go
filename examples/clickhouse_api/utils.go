package clickhouse_api

import (
	"crypto/tls"
	"github.com/xiaoxiaolai/clickhouse-go"
	"github.com/xiaoxiaolai/clickhouse-go/lib/driver"
	clickhouse_tests "github.com/xiaoxiaolai/clickhouse-go/tests"
)

const TestSet string = "examples_clickhouse_api"

func GetNativeConnection(settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (driver.Conn, error) {
	return clickhouse_tests.GetConnection(TestSet, settings, tlsConfig, compression)
}

func GetNativeTestEnvironment() (clickhouse_tests.ClickHouseTestEnvironment, error) {
	return clickhouse_tests.GetTestEnvironment(TestSet)
}

func GetNativeConnectionWithOptions(settings clickhouse.Settings, tlsConfig *tls.Config, compression *clickhouse.Compression) (driver.Conn, error) {
	return clickhouse_tests.GetConnection(TestSet, settings, tlsConfig, compression)
}
