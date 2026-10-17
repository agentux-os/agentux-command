package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/agentux-os/agentux-command/internal/office"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv("AGENTUX_COMMAND_DATABASE_URL"), os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, databaseURL string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("agentux-command-storage-report", flag.ContinueOnError)
	flags.SetOutput(stderr)
	detailed := flags.Bool("detailed", false, "include exact time bounds and per-organization aggregates (may scan the event log)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "unexpected arguments: %v\n", flags.Args())
		return 2
	}
	if strings.TrimSpace(databaseURL) == "" {
		_, _ = fmt.Fprintln(stderr, "AGENTUX_COMMAND_DATABASE_URL is required")
		return 2
	}
	parsedURL, err := url.Parse(databaseURL)
	if err != nil || (parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql") || parsedURL.Hostname() == "" {
		_, _ = fmt.Fprintln(stderr, "AGENTUX_COMMAND_DATABASE_URL is invalid")
		return 2
	}
	db, err := sql.Open("pgx", parsedURL.String())
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "open PostgreSQL connection: %v\n", err)
		return 1
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_, _ = fmt.Fprintf(stderr, "connect to PostgreSQL: %v\n", err)
		return 1
	}
	report, err := office.CollectGrowthReport(ctx, db, *detailed)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "collect Office event growth report: %v\n", err)
		return 1
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "encode Office event growth report: %v\n", err)
		return 1
	}
	if _, err := fmt.Fprintln(stdout, string(encoded)); err != nil {
		_, _ = fmt.Fprintf(stderr, "write Office event growth report: %v\n", err)
		return 1
	}
	return 0
}
