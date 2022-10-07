package sqldb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/spy16/delayq"
)

var _ delayq.DelayQ = (*DelayQ)(nil)

type DelayQ struct {
	db    *sql.DB
	table string
}

func (dq *DelayQ) Enqueue(ctx context.Context, items ...delayq.Item) error {
	insertQuery := fmt.Sprintf(`INSERT INTO %s (id, value, next_at) VALUES ($1, $2, $3)`, dq.table)

	txErr := dq.withTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, insertQuery)
		if err != nil {
			return err
		}

		for _, item := range items {
			if _, err := stmt.Exec(randString(8), item.Value, item.At.Unix()); err != nil {
				return err
			}
		}

		return nil
	})
	if txErr != nil {
		return txErr
	}

	return nil
}

func (dq *DelayQ) Dequeue(ctx context.Context, relativeTo time.Time, fn delayq.Process) error {
	if err := dq.processOne(ctx, relativeTo, fn); err != nil {
		return err
	}
	return nil
}

func (dq *DelayQ) processOne(baseCtx context.Context, t time.Time, fn delayq.Process) error {
	id, item, err := dq.findOneReady(baseCtx, t)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return delayq.ErrNoItem
		}
		return err
	}

	refreshItem := func(ctx context.Context, now time.Time) error {
		refreshQuery := fmt.Sprintf(`UPDATE %s SET next_at=$1 where id=$2`, dq.table)
		_, err := dq.db.ExecContext(ctx, refreshQuery, now.Add(200*time.Millisecond), id)
		return err
	}

	ackItem := func(ctx context.Context) error {
		delQuery := fmt.Sprintf(`DELETE FROM %s WHERE id=$1`, dq.table)
		_, err := dq.db.ExecContext(ctx, delQuery, id)
		return err
	}

	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	// Refresher: This goroutine keeps extending the next_at timestamp of the item
	// to make sure no other worker picks up the same item.
	go func() {
		defer cancel()

		tim := time.NewTicker(300 * time.Millisecond)
		defer tim.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case now := <-tim.C:
				if err := refreshItem(ctx, now); err != nil {
					log.Printf("[ERROR] failed to refresh. discarding job (error=%v)", err)
					return
				}
			}
		}
	}()

	if err := fn(ctx, *item); err != nil {
		cancel()
		return err
	} else if err := ackItem(ctx); err != nil {
		return err
	}
	return nil
}

func (dq *DelayQ) findOneReady(ctx context.Context, t time.Time) (string, *delayq.Item, error) {
	var (
		selectQuery  = fmt.Sprintf(`SELECT id, value, version, next_at FROM %s WHERE next_at<=$1;`, dq.table)
		refreshQuery = fmt.Sprintf(`UPDATE %s SET next_at=$1, version=version+1 where id=$2 and version=$3`, dq.table)
	)

	var id, value string
	var at, version int64

	lockOne := func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, selectQuery, t)
		if err := row.Scan(&id, &value, &version, &at); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return sql.ErrNoRows
			}
			return fmt.Errorf("during scan: %w", err)
		}

		refreshTo := time.Now().Add(300 * time.Millisecond).Unix()
		_, err := tx.ExecContext(ctx, refreshQuery, refreshTo, id, version)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return sql.ErrNoRows
			}
			return fmt.Errorf("during extend: %w", err)
		}
		return nil
	}

	txErr := dq.withTx(ctx, lockOne)
	if txErr != nil {
		return "", nil, txErr
	}

	return id, &delayq.Item{
		At:    time.Unix(at, 0).UTC(),
		Value: value,
	}, nil
}

func (dq *DelayQ) withTx(ctx context.Context, txFn func(tx *sql.Tx) error) error {
	tx, err := dq.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := txFn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func randString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
