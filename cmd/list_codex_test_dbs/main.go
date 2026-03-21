package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type rootConfig struct {
	DB struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Params   string `json:"params"`
	} `json:"db"`
}

type dbItem struct {
	Name    string
	LastDDL time.Time
}

func main() {
	dropAll := flag.Bool("drop-all", false, "drop all codex_test_* databases")
	flag.Parse()

	raw, err := os.ReadFile(filepath.Clean(`..\ctp-future-resources\config\config.json`))
	if err != nil {
		panic(err)
	}
	raw = bytes.TrimPrefix(raw, []byte{0xEF, 0xBB, 0xBF})
	var cfg rootConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		panic(err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema?%s", cfg.DB.User, cfg.DB.Password, cfg.DB.Host, cfg.DB.Port, cfg.DB.Params)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(`
SELECT s.SCHEMA_NAME, COALESCE(MAX(t.CREATE_TIME), '1000-01-01 00:00:00')
FROM information_schema.SCHEMATA s
LEFT JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = s.SCHEMA_NAME
WHERE s.SCHEMA_NAME LIKE 'codex_test\_%'
GROUP BY s.SCHEMA_NAME
`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var items []dbItem
	for rows.Next() {
		var name string
		var lastDDLRaw string
		if err := rows.Scan(&name, &lastDDLRaw); err != nil {
			panic(err)
		}
		lastDDL, err := time.ParseInLocation("2006-01-02 15:04:05", lastDDLRaw, time.Local)
		if err != nil {
			panic(err)
		}
		items = append(items, dbItem{Name: name, LastDDL: lastDDL})
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].LastDDL.Equal(items[j].LastDDL) {
			return items[i].Name > items[j].Name
		}
		return items[i].LastDDL.After(items[j].LastDDL)
	})

	if *dropAll {
		for _, item := range items {
			dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", strings.ReplaceAll(item.Name, "`", "``"))
			if _, err := db.Exec(dropSQL); err != nil {
				panic(err)
			}
			fmt.Printf("dropped\t%s\n", item.Name)
		}
		return
	}

	for idx, item := range items {
		fmt.Printf("%02d\t%s\t%s\n", idx+1, item.LastDDL.Format(time.RFC3339), item.Name)
	}
}
