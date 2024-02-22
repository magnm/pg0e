package conn

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"
)

const LOCK_ID uint64 = 74033

type Control struct {
	Conn *pgx.Conn
}

var ErrNoConnection = errors.New("no connection")
var ErrFailedExclusivity = errors.New("failed to acquire exclusivity")

func getControlDsn() string {
	dsn := os.Getenv("CONTROL_DSN")
	if dsn == "" {
		pgHost := os.Getenv("PGHOST")
		pgPort := os.Getenv("PGPORT")
		if pgHost == "" {
			pgHost = "127.0.0.1"
		}
		if pgPort == "" {
			pgPort = "5432"
		}
		dsn = fmt.Sprintf("postgres://postgres@%s:%s/postgres", pgHost, pgPort)
	}
	return dsn
}

func NewControl() *Control {
	conn, err := pgx.Connect(context.Background(), getControlDsn())
	if err != nil {
		slog.Error("error connecting to control db", "err", err.Error())
	}

	return &Control{Conn: conn}
}

func (c *Control) Close() {
	if c.Conn == nil {
		return
	}
	c.Conn.Close(context.Background())
}

func (c *Control) AttachLock() {
	if c.Conn == nil {
		return
	}
	_, err := c.Conn.Exec(context.Background(), "select pg_catalog.pg_advisory_lock_shared($1)", LOCK_ID)
	if err != nil {
		slog.Error("error acquiring lock", "err", err.Error())
	}
}

func (c *Control) ReleaseLock() {
	if c.Conn == nil {
		return
	}
	_, err := c.Conn.Exec(context.Background(), "select pg_catalog.pg_advisory_unlock_all()")
	if err != nil {
		slog.Error("error releasing lock", "err", err.Error())
	}
}

func (c *Control) GetControlCount() int {
	if c.Conn == nil {
		return -1
	}
	var count int
	err := c.Conn.QueryRow(
		context.Background(),
		"select count(*) from pg_catalog.pg_locks where locktype = 'advisory' and objid = $1",
		LOCK_ID,
	).Scan(&count)
	if err != nil {
		slog.Error("error getting control count", "err", err.Error())
		return -1
	}

	return count
}

func (c *Control) AcquireExclusivity() error {
	if c.Conn == nil {
		return ErrNoConnection
	}
	var locked bool
	err := c.Conn.QueryRow(context.Background(), "select pg_catalog.pg_try_advisory_lock($1)", LOCK_ID).Scan(&locked)
	if err != nil {
		return err
	} else if !locked {
		return ErrFailedExclusivity
	}
	return nil
}
