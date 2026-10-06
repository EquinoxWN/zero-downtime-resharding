// Package localpg starts real PostgreSQL 17 servers on localhost for the integration tests and the
// demo, with logical replication enabled. The binaries are downloaded once into the repository's
// .tmp/pg/cache, and every server's files live in a fresh directory under .tmp/pg/run.
package localpg

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	ep "github.com/fergusstrange/embedded-postgres"
)

// Server is one running PostgreSQL.
type Server struct {
	Name string
	Port uint32
	DSN  string
	db   *ep.EmbeddedPostgres
}

// Cluster is a set of servers started together.
type Cluster struct {
	Servers []*Server
	dir     string
	logs    bytes.Buffer
}

// Start runs one server per name. Every server listens on localhost only, with a password made
// up for this run.
func Start(names ...string) (*Cluster, error) {
	root, err := moduleRoot()
	if err != nil {
		return nil, err
	}
	base := filepath.Join(root, ".tmp", "pg")
	if err := os.MkdirAll(filepath.Join(base, "run"), 0o755); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(filepath.Join(base, "run"), "cluster-")
	if err != nil {
		return nil, err
	}
	secret := make([]byte, 12)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	password := hex.EncodeToString(secret)
	c := &Cluster{dir: dir}
	for _, name := range names {
		port, err := freePort()
		if err != nil {
			c.Stop()
			return nil, err
		}
		cfg := ep.DefaultConfig().Version(ep.V17).Port(port).Password(password).
			CachePath(filepath.Join(base, "cache")).
			RuntimePath(filepath.Join(dir, name, "run")).
			BinariesPath(filepath.Join(dir, name, "bin")).
			DataPath(filepath.Join(dir, name, "data")).
			StartTimeout(2 * time.Minute).
			Logger(io.Writer(&c.logs)).
			StartParameters(map[string]string{
				"listen_addresses":             "localhost",
				"wal_level":                    "logical",
				"max_replication_slots":        "20",
				"max_wal_senders":              "20",
				"max_connections":              "100",
				"wal_receiver_status_interval": "1s",
			})
		db := ep.NewDatabase(cfg)
		if err := db.Start(); err != nil {
			c.Stop()
			return nil, fmt.Errorf("start %s: %w", name, err)
		}
		c.Servers = append(c.Servers, &Server{
			Name: name, Port: port, db: db,
			DSN: fmt.Sprintf("postgres://postgres:%s@localhost:%d/postgres?sslmode=disable", password, port),
		})
	}
	return c, nil
}

// Stop shuts every server down and deletes their files.
func (c *Cluster) Stop() {
	for _, s := range c.Servers {
		_ = s.db.Stop()
	}
	if c.dir != "" {
		_ = os.RemoveAll(c.dir)
	}
}

// Logs returns what the servers logged.
func (c *Cluster) Logs() string { return c.logs.String() }

func freePort() (uint32, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return uint32(l.Addr().(*net.TCPAddr).Port), nil
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found above the working directory")
		}
		dir = parent
	}
}
