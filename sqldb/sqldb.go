package sqldb

import (
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
)

//go:embed schema.sql
var schemaTemplate string

func New(db *sql.DB, queueName string) (*DelayQ, error) {
	dq := &DelayQ{
		db:    db,
		table: fmt.Sprintf("delayq_%s", queueName),
	}
	if err := dq.initSchema(); err != nil {
		return nil, err
	}
	return dq, nil
}

func (dq *DelayQ) initSchema() error {
	tableSchema := strings.Replace(schemaTemplate, "__name__", dq.table, -1)
	_, err := dq.db.Exec(tableSchema)
	return err
}
