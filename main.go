package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/anytoe/chsync/internal/models"
	"github.com/anytoe/chsync/internal/repositories/clickhouse"
	"github.com/anytoe/chsync/internal/repositories/docker"
	"github.com/maxrichie5/go-sqlfmt/sqlfmt"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "chsync",
	Short: "ClickHouse schema synchronization tool",
	Long:  `A tool for comparing and syncing schemas between ClickHouse instances`,
}

var diffCmd = &cobra.Command{
	Use:          "diff",
	Short:        "Compare schemas and generate migration",
	RunE:         runDiff,
	SilenceUsage: true,
}

var snapshotCmd = &cobra.Command{
	Use:          "snapshot",
	Short:        "Snapshot schema from a ClickHouse instance",
	RunE:         runSnapshot,
	SilenceUsage: true,
}

var (
	fromSource      string
	toSource        string
	diffOutFile     string
	snapshotOutFile string
	diffOnlyDbs     string
	diffSkipDbs     string
	diffOnlyTables  string
	diffSkipTables  string

	snapshotOnlyDbs    string
	snapshotSkipDbs    string
	snapshotOnlyTables string
	snapshotSkipTables string
	snapshotLogDir     string
	verify             bool
	verifyVersion      string
	verbose            bool
)

func init() {
	diffCmd.Flags().StringVar(&fromSource, "from", "", "Source: instance DSN or .sql file")
	diffCmd.Flags().StringVar(&toSource, "to", "", "Target: instance DSN or .sql file")
	diffCmd.Flags().StringVar(&diffOutFile, "out", "migration.sql", "Output migration file")
	diffCmd.Flags().StringVar(&diffOnlyDbs, "only-dbs", "", "Comma-separated databases to include (all others ignored)")
	diffCmd.Flags().StringVar(&diffSkipDbs, "skip-dbs", "", "Comma-separated databases to skip")
	diffCmd.Flags().StringVar(&diffOnlyTables, "only-tables", "", "Comma-separated table names to include (all others ignored; combine with --only-dbs to scope by database)")
	diffCmd.Flags().StringVar(&diffSkipTables, "skip-tables", "", "Comma-separated table names to skip")
	_ = diffCmd.MarkFlagRequired("from")
	_ = diffCmd.MarkFlagRequired("to")

	snapshotCmd.Flags().StringVar(&fromSource, "from", "", "Source instance DSN")
	snapshotCmd.Flags().StringVar(&snapshotOutFile, "out", "schema.sql", "Output schema file")
	snapshotCmd.Flags().StringVar(&snapshotOnlyDbs, "only-dbs", "", "Comma-separated databases to include (all others ignored)")
	snapshotCmd.Flags().StringVar(&snapshotSkipDbs, "skip-dbs", "", "Comma-separated databases to skip")
	snapshotCmd.Flags().StringVar(&snapshotOnlyTables, "only-tables", "", "Comma-separated table names to include (all others ignored; combine with --only-dbs to scope by database)")
	snapshotCmd.Flags().StringVar(&snapshotSkipTables, "skip-tables", "", "Comma-separated table names to skip")
	snapshotCmd.Flags().BoolVar(&verify, "verify", false, "Verify exported schema in Docker container (requires Docker)")
	snapshotCmd.Flags().StringVar(&verifyVersion, "verify-version", "latest", "ClickHouse version for verification (default: latest)")
	snapshotCmd.Flags().StringVar(&snapshotLogDir, "log", "", "Directory to write timestamped migration log entries (diffs old --out vs new schema; requires Docker)")
	_ = snapshotCmd.MarkFlagRequired("from")

	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable informational output (errors are always shown)")

	rootCmd.AddCommand(diffCmd, snapshotCmd)
}

// isDSN reports whether s is a connection string rather than a file path.
func isDSN(s string) bool {
	return strings.Contains(s, "://")
}

// resolveSource connects to a ClickHouse instance identified by source, which is either
// a DSN (http://, clickhouse://, ...) or a path to a .sql file.
// When source is a file, a temporary Docker container is started and the schema is loaded into it.
// The caller must call ch.Close() when done — for file sources, that also terminates the container.
func resolveSource(ctx context.Context, source, role string) (*clickhouse.Client, error) {
	if isDSN(source) {
		return clickhouse.Connect(ctx, source)
	}

	data, err := os.ReadFile(source)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", source, err)
	}
	stmts, err := models.ParseFile(string(data))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", source, err)
	}
	name := fmt.Sprintf("chsync-%s-%s", role, time.Now().Format("20060102-150405"))
	dm := docker.Manager{}
	return dm.StartWithSchema(ctx, stmts, "latest", name)
}

func runDiff(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	infof("Comparing schemas:\n  From: %s\n  To: %s\n  Output: %s\n", redactDSN(fromSource), redactDSN(toSource), diffOutFile)

	// Check Docker availability upfront if either source is a file
	if !isDSN(fromSource) || !isDSN(toSource) {
		dm := docker.Manager{}
		installed, running := dm.IsDockerAvailable()
		if !installed {
			return fmt.Errorf("Docker is not installed or not in PATH.\n\n" +
				"The diff command requires Docker when using .sql files as input.\n" +
				"Install Docker from: https://docs.docker.com/get-docker/")
		}
		if !running {
			return fmt.Errorf("Docker is installed but not running.\n\n" +
				"Please start Docker and try again.")
		}
	}

	fromCh, err := resolveSource(ctx, fromSource, "from")
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer fromCh.Close()
	infof("Connected to source\n")

	toCh, err := resolveSource(ctx, toSource, "to")
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer toCh.Close()
	infof("Connected to target\n")

	filter := clickhouse.Filter{
		OnlyDbs:    parseList(diffOnlyDbs),
		SkipDbs:    parseList(diffSkipDbs),
		OnlyTables: parseList(diffOnlyTables),
		SkipTables: parseList(diffSkipTables),
	}
	output, err := generateMigrationSQL(ctx, fromCh, toCh, filter)
	if err != nil {
		return err
	}

	if output == "" {
		fmt.Println("No differences found.")
		return nil
	}

	if err := os.WriteFile(diffOutFile, []byte(output), 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	infof("Migration written to %s\n", diffOutFile)
	return nil
}

// generateMigrationSQL loads the schemas from both clients, diffs them, and returns
// the migration SQL. Returns an empty string when there are no differences.
func generateMigrationSQL(ctx context.Context, fromCh, toCh *clickhouse.Client, filter clickhouse.Filter) (string, error) {
	fromSchema, err := fromCh.LoadSchema(ctx, filter)
	if err != nil {
		return "", fmt.Errorf("failed to load source schema: %w", err)
	}
	infof("Loaded source schema\n")

	toSchema, err := toCh.LoadSchema(ctx, filter)
	if err != nil {
		return "", fmt.Errorf("failed to load target schema: %w", err)
	}
	infof("Loaded target schema\n")

	combined := models.NewCombinedSchema(*fromSchema, *toSchema)
	infof("Compared schemas\n")

	// Load type aliases from the source instance (used to normalise column type comparisons).
	// Errors are non-fatal: older ClickHouse may not have the table.
	typeAliases, err := fromCh.LoadTypeAliases(ctx)
	if err != nil {
		infof("Warning: could not load type aliases: %v\n", err)
		typeAliases = nil
	}
	if toAliases, err := toCh.LoadTypeAliases(ctx); err == nil {
		for k, v := range toAliases {
			if _, exists := typeAliases[k]; !exists {
				typeAliases[k] = v
			}
		}
	}

	generator := models.NewSyncPlanGenerator(models.GeneratorConfig{
		TableRenameSimilarityThreshold:  0.80,
		ColumnRenameSimilarityThreshold: 0.70,
		TypeAliases:                     typeAliases,
	})
	plan := generator.Generate(combined)
	infof("Generated sync plan\n")

	var output string
	if len(plan.Strategies) > 0 {
		for _, op := range plan.Strategies[0].Operations {
			output += "-- " + op.Explanation + "\n"
			for _, stmt := range op.Statements {
				output += stmt + "\n"
			}
			output += "\n"
		}
		output = sqlfmt.Format(output)
	}

	if output != "" && !strings.HasSuffix(output, "\n") {
		output += "\n"
	}
	return output, nil
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	infof("Exporting schema:\n  From: %s\n  Output: %s\n", redactDSN(fromSource), snapshotOutFile)

	ch, err := clickhouse.Connect(ctx, fromSource)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer ch.Close()
	infof("Connected to ClickHouse\n")

	stmts, err := ch.ExportSQL(ctx, clickhouse.Filter{
		OnlyDbs:    parseList(snapshotOnlyDbs),
		SkipDbs:    parseList(snapshotSkipDbs),
		OnlyTables: parseList(snapshotOnlyTables),
		SkipTables: parseList(snapshotSkipTables),
	})
	if err != nil {
		return fmt.Errorf("failed to export schema: %w", err)
	}
	infof("Exported schema as SQL\n")

	if verify {
		dm := docker.Manager{}
		installed, running := dm.IsDockerAvailable()
		if !installed {
			return fmt.Errorf("Docker is not installed or not in PATH.\n\n" +
				"The --verify flag requires Docker to be installed.\n" +
				"Install Docker from: https://docs.docker.com/get-docker/\n\n" +
				"Alternatively, run without --verify to skip schema verification.")
		}
		if !running {
			return fmt.Errorf("Docker is installed but not running.\n\n" +
				"Please start Docker and try again.\n\n" +
				"Alternatively, run without --verify to skip schema verification.")
		}
		infof("Verifying schema with Docker (version: %s)\n", verifyVersion)
		if err := dm.VerifyWithDocker(ctx, stmts, verifyVersion); err != nil {
			return fmt.Errorf("verification failed: %w", err)
		}
		infof("Verification passed\n")
	}

	newSQL := sqlfmt.Format(stmts.ToStatements())
	if !strings.HasSuffix(newSQL, "\n") {
		newSQL += "\n"
	}

	// Write a changelog entry before overwriting --out, so the old snapshot
	// is preserved on failure and we can diff old vs new.
	if snapshotLogDir != "" {
		if err := writeChangelog(ctx, stmts, newSQL); err != nil {
			return fmt.Errorf("failed to write changelog: %w", err)
		}
	}

	if err := atomicWriteFile(snapshotOutFile, []byte(newSQL)); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	infof("Schema exported successfully to %s\n", snapshotOutFile)

	return nil
}

// writeChangelog produces a timestamped migration log entry in snapshotLogDir.
// On the first run (no existing snapshot at snapshotOutFile) the full new schema
// is written as the initial entry. Otherwise both old and new snapshots are
// loaded into temporary Docker containers and diffed; the resulting migration
// SQL is written. If there are no differences, no file is written.
func writeChangelog(ctx context.Context, newStmts *models.SQLStatements, newSQL string) error {
	if err := os.MkdirAll(snapshotLogDir, 0755); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}

	timestamp := time.Now().Format("2006-01-02T150405")
	logFile := filepath.Join(snapshotLogDir, timestamp+".sql")

	oldData, err := os.ReadFile(snapshotOutFile)
	if os.IsNotExist(err) {
		infof("No existing snapshot at %s; writing initial changelog entry to %s\n", snapshotOutFile, logFile)
		return os.WriteFile(logFile, []byte(newSQL), 0644)
	}
	if err != nil {
		return fmt.Errorf("read existing snapshot: %w", err)
	}

	dm := docker.Manager{}
	installed, running := dm.IsDockerAvailable()
	if !installed {
		return fmt.Errorf("Docker is not installed or not in PATH.\n\n" +
			"The --log flag requires Docker to diff old and new snapshots.\n" +
			"Install Docker from: https://docs.docker.com/get-docker/")
	}
	if !running {
		return fmt.Errorf("Docker is installed but not running.\n\n" +
			"Please start Docker and try again.")
	}

	oldStmts, err := models.ParseFile(string(oldData))
	if err != nil {
		return fmt.Errorf("parse existing snapshot %s: %w", snapshotOutFile, err)
	}

	stamp := time.Now().Format("20060102-150405")
	infof("Loading old snapshot in Docker (version: %s)\n", verifyVersion)
	oldCh, err := dm.StartWithSchema(ctx, oldStmts, verifyVersion, "chsync-log-old-"+stamp)
	if err != nil {
		return fmt.Errorf("load old snapshot in Docker: %w", err)
	}
	defer oldCh.Close()

	infof("Loading new snapshot in Docker (version: %s)\n", verifyVersion)
	newCh, err := dm.StartWithSchema(ctx, newStmts, verifyVersion, "chsync-log-new-"+stamp)
	if err != nil {
		return fmt.Errorf("load new snapshot in Docker: %w", err)
	}
	defer newCh.Close()

	output, err := generateMigrationSQL(ctx, oldCh, newCh, clickhouse.Filter{})
	if err != nil {
		return err
	}

	if output == "" {
		infof("No schema differences; skipping changelog entry\n")
		return nil
	}

	if err := os.WriteFile(logFile, []byte(output), 0644); err != nil {
		return fmt.Errorf("write changelog: %w", err)
	}
	infof("Changelog written to %s\n", logFile)
	return nil
}

// atomicWriteFile writes data to path via a sibling .tmp file then renames it
// into place, so the destination is either the old contents or the new — never partial.
func atomicWriteFile(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func parseList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func infof(format string, args ...any) {
	if verbose {
		fmt.Printf(format, args...)
	}
}

func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	u.User = nil
	u.RawQuery = ""
	return u.String()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
