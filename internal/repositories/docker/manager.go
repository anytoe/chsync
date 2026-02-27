package docker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/anytoe/chsync/internal/models"
	"github.com/anytoe/chsync/internal/repositories/clickhouse"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	containerUser     = "default"
	containerPassword = "clickhouse"
	containerHTTPPort = "8123"
)

type Manager struct {
}

// IsDockerAvailable checks whether Docker is installed and running.
// Returns (installed, running).
func (dm *Manager) IsDockerAvailable() (installed bool, running bool) {
	_, err := exec.LookPath("docker")
	if err != nil {
		return false, false
	}
	cmd := exec.Command("docker", "info")
	if err := cmd.Run(); err != nil {
		return true, false
	}
	return true, true
}

// StartWithSchema spins up a ClickHouse container, loads the given statements, and returns
// a connected client. The caller must call the returned cleanup function when done.
func (dm *Manager) StartWithSchema(ctx context.Context, stmts *models.SQLStatements, version, name string) (*clickhouse.Client, func(), error) {
	if version == "" {
		version = "latest"
	}

	req := testcontainers.ContainerRequest{
		Name:         name,
		Image:        fmt.Sprintf("clickhouse/clickhouse-server:%s", version),
		ExposedPorts: []string{"9000/tcp", containerHTTPPort + "/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_USER":     containerUser,
			"CLICKHOUSE_PASSWORD": containerPassword,
		},
		WaitingFor: wait.ForHTTP("/ping").
			WithPort(containerHTTPPort + "/tcp").
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start container: %w", err)
	}

	stopContainer := func() {
		if err := container.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to terminate container %q: %v\n", name, err)
		}
	}

	host, err := container.Host(ctx)
	if err != nil {
		stopContainer()
		return nil, nil, fmt.Errorf("failed to get container host: %w", err)
	}

	port, err := container.MappedPort(ctx, containerHTTPPort)
	if err != nil {
		stopContainer()
		return nil, nil, fmt.Errorf("failed to get mapped port: %w", err)
	}

	// Connect to ClickHouse (http:// for local container without TLS)
	dsn := fmt.Sprintf("http://%s:%s@%s:%s/%s", containerUser, containerPassword, host, port.Port(), containerUser)

	ch, err := clickhouse.Connect(ctx, dsn)
	if err != nil {
		stopContainer()
		return nil, nil, fmt.Errorf("failed to connect to container: %w", err)
	}

	for i, stmt := range stmts.StatementsCleaned {
		stmt = trimStatement(stmt)
		if stmt == "" {
			continue
		}
		_, err = ch.Query(ctx, stmt)
		if err != nil {
			ch.Close()
			stopContainer()
			return nil, nil, fmt.Errorf("failed to execute statement %d: %w\nStatement: %s", i+1, err, stmt)
		}
	}

	cleanup := func() {
		ch.Close()
		stopContainer()
	}
	return ch, cleanup, nil
}

// VerifyWithDocker spins up a temporary ClickHouse container and verifies the SQL statements.
func (dm *Manager) VerifyWithDocker(ctx context.Context, stmts *models.SQLStatements, version string) error {
	name := fmt.Sprintf("chsync-%s", time.Now().Format("20060102-150405"))
	_, cleanup, err := dm.StartWithSchema(ctx, stmts, version, name)
	if err != nil {
		return err
	}
	defer cleanup()
	return nil
}

// splitLines splits text into lines
func splitLines(text string) []string {
	lines := []string{}
	start := 0

	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			lines = append(lines, text[start:i])
			start = i + 1
		}
	}

	if start < len(text) {
		lines = append(lines, text[start:])
	}

	return lines
}

// trimStatement removes leading/trailing whitespace and comments
func trimStatement(stmt string) string {
	lines := splitLines(stmt)
	var cleaned []string

	for _, line := range lines {
		// Skip comment lines
		trimmed := trimSpace(line)
		if len(trimmed) >= 2 && trimmed[0] == '-' && trimmed[1] == '-' {
			continue
		}
		cleaned = append(cleaned, line)
	}

	result := ""
	for _, line := range cleaned {
		result += line + "\n"
	}

	return trimSpace(result)
}

// trimSpace removes leading and trailing whitespace
func trimSpace(s string) string {
	start := 0
	end := len(s)

	for start < end && isSpace(s[start]) {
		start++
	}

	for end > start && isSpace(s[end-1]) {
		end--
	}

	return s[start:end]
}

// isSpace checks if character is whitespace
func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
