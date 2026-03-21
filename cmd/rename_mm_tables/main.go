package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"

	"ctp-future-kline/internal/config"
	dbx "ctp-future-kline/internal/db"
)

const (
	oldPrefix = "future_kline_instrument_1m_mm_"
	newPrefix = "future_kline_instrument_mm_"
)

type renamePlan struct {
	OldName string
	NewName string
}

func main() {
	mysqlHost := flag.String("mysql-host", "localhost", "mysql host")
	mysqlPort := flag.Int("mysql-port", 3306, "mysql port")
	mysqlUser := flag.String("mysql-user", "root", "mysql user")
	mysqlPassword := flag.String("mysql-password", "", "mysql password")
	mysqlDB := flag.String("mysql-db", "future_kline", "mysql database")
	execute := flag.Bool("execute", false, "execute rename; default is dry-run")
	flag.Parse()

	cfg := config.DBConfig{
		Driver:   "mysql",
		Host:     *mysqlHost,
		Port:     *mysqlPort,
		User:     *mysqlUser,
		Password: *mysqlPassword,
		Database: *mysqlDB,
		Params:   "parseTime=true&loc=Local&multiStatements=false",
	}

	db, err := dbx.Open(dbx.BuildDSN(cfg))
	if err != nil {
		fatalf("open mysql failed: %v", err)
	}
	defer db.Close()

	plans, err := buildPlans(db)
	if err != nil {
		fatalf("scan old mm tables failed: %v", err)
	}
	if len(plans) == 0 {
		fmt.Println("no old mm tables found, nothing to do")
		return
	}

	fmt.Printf("found %d old mm tables:\n", len(plans))
	for _, p := range plans {
		fmt.Printf("  %s -> %s\n", p.OldName, p.NewName)
	}

	if !*execute {
		fmt.Println("dry-run only. add -execute to apply changes.")
		return
	}

	if err := executePlans(db, plans); err != nil {
		fatalf("rename failed: %v", err)
	}
	fmt.Printf("rename done, renamed=%d\n", len(plans))
}

func buildPlans(db *sql.DB) ([]renamePlan, error) {
	rows, err := db.Query(`
SELECT table_name
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name LIKE 'future_kline_instrument_1m_mm_%'
ORDER BY table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]renamePlan, 0, 32)
	for rows.Next() {
		var oldName string
		if err := rows.Scan(&oldName); err != nil {
			return nil, err
		}
		v := strings.TrimPrefix(oldName, oldPrefix)
		v = sanitizeIdent(v)
		if v == "" {
			continue
		}
		newName := newPrefix + v
		exists, err := tableExists(db, newName)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("target table already exists: %s (from %s)", newName, oldName)
		}
		out = append(out, renamePlan{OldName: oldName, NewName: newName})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func executePlans(db *sql.DB, plans []renamePlan) error {
	for _, p := range plans {
		stmt := fmt.Sprintf(`ALTER TABLE "%s" RENAME TO "%s"`, p.OldName, p.NewName)
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("%s -> %s: %w", p.OldName, p.NewName, err)
		}
		fmt.Printf("renamed: %s -> %s\n", p.OldName, p.NewName)
	}
	return nil
}

func tableExists(db *sql.DB, tableName string) (bool, error) {
	var cnt int
	err := db.QueryRow(`
SELECT COUNT(1)
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name = ?`, tableName).Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func sanitizeIdent(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, ch := range strings.ToLower(strings.TrimSpace(s)) {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
