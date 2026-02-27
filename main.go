package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
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
	diffOnlyDbs    string
	diffSkipDbs    string
	diffOnlyTables string
	diffSkipTables string

	snapshotOnlyDbs    string
	snapshotSkipDbs    string
	snapshotOnlyTables string
	snapshotSkipTables string
	verify          bool
	verifyVersion   string
	verbose         bool
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
// The caller must call the returned cleanup function when done.
func resolveSource(ctx context.Context, source, role string) (*clickhouse.Client, func(), error) {
	if isDSN(source) {
		ch, err := clickhouse.Connect(ctx, source)
		if err != nil {
			return nil, nil, err
		}
		return ch, func() { ch.Close() }, nil
	}

	data, err := os.ReadFile(source)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read %s: %w", source, err)
	}
	stmts, err := models.ParseFile(string(data))
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", source, err)
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

	fromCh, fromCleanup, err := resolveSource(ctx, fromSource, "from")
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer fromCleanup()
	infof("Connected to source\n")

	toCh, toCleanup, err := resolveSource(ctx, toSource, "to")
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer toCleanup()
	infof("Connected to target\n")

	// Load schemas
	filter := clickhouse.Filter{
		OnlyDbs:    parseList(diffOnlyDbs),
		SkipDbs:    parseList(diffSkipDbs),
		OnlyTables: parseList(diffOnlyTables),
		SkipTables: parseList(diffSkipTables),
	}
	fromSchema, err := fromCh.LoadSchema(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to load source schema: %w", err)
	}
	infof("Loaded source schema\n")

	toSchema, err := toCh.LoadSchema(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to load target schema: %w", err)
	}
	infof("Loaded target schema\n")

	// Compare schemas
	combined := models.NewCombinedSchema(*fromSchema, *toSchema)
	infof("Compared schemas\n")

	// Load type aliases from the source instance (used to normalise column type comparisons).
	// Errors are non-fatal: if the query fails (e.g. older ClickHouse without the table),
	// we simply proceed without alias resolution.
	typeAliases, err := fromCh.LoadTypeAliases(ctx)
	if err != nil {
		infof("Warning: could not load type aliases: %v\n", err)
		typeAliases = nil
	}
	// Merge aliases from the target instance so both servers' alias vocabularies are covered.
	if toAliases, err := toCh.LoadTypeAliases(ctx); err == nil {
		for k, v := range toAliases {
			if _, exists := typeAliases[k]; !exists {
				typeAliases[k] = v
			}
		}
	}

	// Generate sync plan
	generator := models.NewSyncPlanGenerator(models.GeneratorConfig{
		TableRenameSimilarityThreshold:  0.80,
		ColumnRenameSimilarityThreshold: 0.70,
		TypeAliases:                     typeAliases,
	})
	plan := generator.Generate(combined)
	infof("Generated sync plan\n")

	// Collect all SQL statements from the first strategy
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

	if output == "" {
		fmt.Println("No differences found.")
	}

	// Always write (or truncate) the output file
	if output != "" && !strings.HasSuffix(output, "\n") {
		output += "\n"
	}
	if err := os.WriteFile(diffOutFile, []byte(output), 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	infof("Migration written to %s\n", diffOutFile)
	return nil
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	infof("Exporting schema:\n  From: %s\n  Output: %s\n", redactDSN(fromSource), snapshotOutFile)

	// Connect to ClickHouse
	ch, err := clickhouse.Connect(ctx, fromSource)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer ch.Close()
	infof("Connected to ClickHouse\n")

	// Extract schema as SQL
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

	// Verify with Docker if requested
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

	// Write to output file
	s := stmts.ToStatements()
	s = sqlfmt.Format(s)
	if !strings.HasSuffix(s, "\n") {
		s += "\n"
	}
	if err := os.WriteFile(snapshotOutFile, []byte(s), 0644); err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	infof("Schema exported successfully to %s\n", snapshotOutFile)

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
