package office

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	version int
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}
	migrations := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("migration filename %q must start with a numeric version", entry.Name())
		}
		version, err := strconv.Atoi(prefix)
		if err != nil || version < 1 {
			return nil, fmt.Errorf("migration filename %q has an invalid version", entry.Name())
		}
		contents, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, migration{version: version, name: entry.Name(), sql: string(contents)})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })
	for i := 1; i < len(migrations); i++ {
		if migrations[i].version == migrations[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %d", migrations[i].version)
		}
	}
	return migrations, nil
}

func MigrationVersions() []int {
	migrations, err := loadMigrations()
	if err != nil {
		return nil
	}
	versions := make([]int, len(migrations))
	for i, item := range migrations {
		versions[i] = item.version
	}
	return versions
}

func Migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("database is required")
	}
	migrations, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS command_schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	for _, item := range migrations {
		if err := applyMigration(ctx, db, item); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, db *sql.DB, item migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %d: %w", item.version, err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(842119)`); err != nil {
		return fmt.Errorf("lock migration %d: %w", item.version, err)
	}
	var applied bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM command_schema_migrations WHERE version = $1)`, item.version).Scan(&applied); err != nil {
		return fmt.Errorf("read migration %d: %w", item.version, err)
	}
	if applied {
		return tx.Commit()
	}
	for _, statement := range strings.Split(item.sql, ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("execute migration %d (%s): %w", item.version, item.name, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO command_schema_migrations (version, name) VALUES ($1, $2)`, item.version, item.name); err != nil {
		return fmt.Errorf("record migration %d: %w", item.version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %d: %w", item.version, err)
	}
	return nil
}
