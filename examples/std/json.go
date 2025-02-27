package std

import (
	"fmt"
	"github.com/xiaoxiaolai/clickhouse-go"
	"github.com/xiaoxiaolai/clickhouse-go/tests/std"
	clickhouse_tests "github.com/xiaoxiaolai/clickhouse-go/tests/std"
)

type Releases struct {
	Version string
}

type Repository struct {
	URL      string `json:"url"`
	Releases []Releases
}

type Achievement struct {
	Name string
}

type Account struct {
	Id            uint32
	Name          string
	Organizations []string `json:"orgs"`
	Repositories  []Repository
	Achievement   Achievement
}

type GithubEvent struct {
	Title        string
	Type         string
	Assignee     Account  `json:"assignee"`
	Labels       []string `json:"labels"`
	Contributors []Account
	// should not be exported
	createdAt string
}

func JSONInsertRead() error {
	conn, err := GetStdOpenDBConnection(clickhouse.Native, clickhouse.Settings{
		"allow_experimental_object_type": 1,
	}, nil, nil)
	if err != nil {
		return err
	}
	if !std.CheckMinServerVersion(conn, 22, 6, 1) {
		return nil
	}
	conn.Exec("DROP TABLE example")
	const ddl = `
		CREATE TABLE example (
			  event JSON
		) Engine Memory
		`
	defer func() {
		conn.Exec("DROP TABLE example")
	}()
	if _, err := conn.Exec(ddl); err != nil {
		return err
	}
	scope, err := conn.Begin()
	if err != nil {
		return err
	}
	batch, err := scope.Prepare("INSERT INTO example")
	if err != nil {
		return err
	}
	col1Data := GithubEvent{
		Title: "Document JSON support",
		Type:  "Issue",
		Assignee: Account{
			Id:            1244,
			Name:          "Geoff",
			Achievement:   Achievement{Name: "Mars Star"},
			Repositories:  []Repository{{URL: "https://github.com/ClickHouse/clickhouse-python", Releases: []Releases{{Version: "1.0.0"}, {Version: "1.1.0"}}}, {URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}},
			Organizations: []string{"Support Engineer", "Integrations"},
		},
		Labels: []string{"Help wanted"},
		Contributors: []Account{
			{Id: 2244, Achievement: Achievement{Name: "Adding JSON to go driver"}, Organizations: []string{"Support Engineer", "Consulting", "PM", "Integrations"}, Name: "Dale", Repositories: []Repository{{URL: "https://github.com/ClickHouse/clickhouse-go", Releases: []Releases{{Version: "2.0.0"}, {Version: "2.1.0"}}}, {URL: "https://github.com/grafana/clickhouse", Releases: []Releases{{Version: "1.2.0"}, {Version: "1.3.0"}}}}},
			{Id: 2344, Achievement: Achievement{Name: "Managing S3 buckets"}, Organizations: []string{"Support Engineer", "Consulting"}, Name: "Melyvn", Repositories: []Repository{{URL: "https://github.com/ClickHouse/support", Releases: []Releases{{Version: "1.0.0"}, {Version: "2.3.0"}, {Version: "2.4.0"}}}}},
		},
	}
	if _, err = batch.Exec(col1Data); err != nil {
		return err
	}
	if err = scope.Commit(); err != nil {
		return err
	}
	// must pass interface{} - maps must be strongly typed so map[string]interface{} wont work - it wont convert
	var event interface{}
	rows := conn.QueryRow("SELECT * FROM example")
	if err = rows.Scan(&event); err != nil {
		return err
	}
	fmt.Println(clickhouse_tests.ToJson(event))
	// again pass interface{} for anthing other than primitives
	rows = conn.QueryRow("SELECT event.assignee.Achievement FROM example")
	var achievement interface{}
	if err = rows.Scan(&achievement); err != nil {
		return err
	}
	fmt.Println(clickhouse_tests.ToJson(event))
	rows = conn.QueryRow("SELECT event.assignee.Repositories FROM example")
	var repositories interface{}
	if err = rows.Scan(&repositories); err != nil {
		return err
	}
	return nil
}
