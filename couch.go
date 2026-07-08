// Package couch provides a small wrapper around the kivik CouchDB driver to
// make it easier to configure connections and persist models.
package couch

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kivik/kivik/v4"
	_ "github.com/go-kivik/kivik/v4/couchdb" // CouchDB driver
)

const driverName = "couch"

// Client wraps around a kivik package Client and helps make it
// easier to configure the connection and prepare the database.
type Client struct {
	conf   *Config
	client *kivik.Client
}

// New provides a new instance of the default CouchDB client. This
// call will block until the server responds or the context causes
// a timeout.
func New(conf *Config, opts ...kivik.Option) (*Client, error) {
	c := new(Client)
	c.conf = conf
	var err error
	c.client, err = kivik.New(driverName, conf.baseURL(), opts...)
	if err != nil {
		return nil, fmt.Errorf("couch: %w", err)
	}
	return c, nil
}

// Ping attempts to establish a connection and will block and retry
// for any timeouts or network errors. This is recommended to be used
// after the client has been initialized to ensure the connection
// is ready to use.
func (c *Client) Ping(ctx context.Context) error {
	const attempts = 10
	var err error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			// Back off between attempts (not before the first, not after the last).
			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if _, err = c.client.Ping(ctx); err == nil {
			return nil
		}
		switch kivik.HTTPStatus(err) {
		case 408, 504, 0:
			// Transient: request timeouts and transport/network errors
			// (status 0) — the server may still be coming up. Retry.
		default:
			// A definitive HTTP status (e.g. 401): don't retry.
			return err
		}
	}
	return fmt.Errorf("couch: ping failed after %d attempts: %w", attempts, err)
}

// DB is used to provide a database instance at the provided name.
func (c *Client) DB(name string) *kivik.DB {
	name = c.conf.db(name)
	return c.client.DB(name)
}

// SyncDesigns ensures the database is up to date with the latest design documents.
func (c *Client) SyncDesigns(ctx context.Context, db *kivik.DB, designs []*Design) error {
	for _, design := range designs {
		if err := design.Sync(ctx, db); err != nil {
			return err
		}
	}
	return nil
}

// Create checks that the database already exists, or creates it
// if required.
func (c *Client) Create(ctx context.Context, db *kivik.DB, opts ...kivik.Option) error {
	ok, err := c.client.DBExists(ctx, db.Name())
	if err != nil {
		return err
	}
	if !ok {
		// db doesn't exist, create it
		if err = c.client.CreateDB(ctx, db.Name(), opts...); err != nil {
			return err
		}
	}
	return nil
}
