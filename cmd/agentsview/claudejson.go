// ABOUTME: CLI subcommand that discovers and syncs older Claude JSON
// ABOUTME: cache session files into the database.
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/wesm/agentsview/internal/config"
	"github.com/wesm/agentsview/internal/db"
	"github.com/wesm/agentsview/internal/parser"
	"github.com/wesm/agentsview/internal/sync"
)

func runClaudeJSON(args []string) {
	if len(args) == 0 || args[0] != "sync" {
		fmt.Fprintf(os.Stderr, `Usage: agentsview claudejson <subcommand>

Subcommands:
  sync    Discover and sync older Claude JSON cache session files

`)
		os.Exit(1)
	}

	appCfg, err := config.LoadMinimal()
	if err != nil {
		log.Fatalf("loading config: %v", err)
	}

	if err := os.MkdirAll(appCfg.DataDir, 0o755); err != nil {
		log.Fatalf("creating data dir: %v", err)
	}

	setupLogFile(appCfg.DataDir)

	database, err := db.Open(appCfg.DBPath)
	if err != nil {
		fatal("opening database: %v", err)
	}
	defer database.Close()

	claudeDirs := appCfg.ResolveDirs(parser.AgentClaude)
	if len(claudeDirs) == 0 {
		fmt.Fprintln(os.Stderr, "warning: no Claude project directories configured")
	}

	var allFiles []parser.DiscoveredFile
	for _, dir := range claudeDirs {
		files := parser.DiscoverClaudeCacheSessions(dir)
		allFiles = append(allFiles, files...)
	}

	fmt.Printf("Discovered %d Claude JSON cache session files\n", len(allFiles))
	if len(allFiles) == 0 {
		return
	}

	engine := sync.NewEngine(database, sync.EngineConfig{
		AgentDirs: appCfg.AgentDirs,
		Machine:   "local",
	})

	synced, errs := engine.SyncCacheFiles(allFiles)

	fmt.Printf("Synced %d sessions", synced)
	if len(errs) > 0 {
		fmt.Printf(", %d errors", len(errs))
	}
	fmt.Println()

	for _, e := range errs {
		fmt.Fprintf(os.Stderr, "error: %v\n", e)
	}
}
