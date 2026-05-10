package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client wraps a ClickHouse connection
type Client struct {
	conn    driver.Conn
	onClose func()
}

// SetOnClose registers a hook invoked after the connection is closed.
// Used by the docker manager to terminate the underlying container when
// the client owns its lifecycle.
func (c *Client) SetOnClose(fn func()) {
	c.onClose = fn
}

// Connect establishes a connection to ClickHouse
// DSN format: https://username:password@host:port?secure=true
func Connect(ctx context.Context, dsn string) (*Client, error) {
	// Convert clickhouse:// to https:// for HTTP protocol
	if strings.HasPrefix(dsn, "clickhouse://") {
		dsn = "https://" + strings.TrimPrefix(dsn, "clickhouse://")
		if !strings.Contains(dsn, "secure=") {
			if strings.Contains(dsn, "?") {
				dsn += "&secure=true"
			} else {
				dsn += "?secure=true"
			}
		}
	}

	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping: %w", err)
	}

	return &Client{conn: conn}, nil
}

// Close closes the connection and runs any registered onClose hook.
func (c *Client) Close() error {
	err := c.conn.Close()
	if c.onClose != nil {
		c.onClose()
	}
	return err
}

// Query executes a query and returns rows
func (c *Client) Query(ctx context.Context, query string) (driver.Rows, error) {
	return c.conn.Query(ctx, query)
}
