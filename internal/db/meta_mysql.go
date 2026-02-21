package db

import "database/sql"

func CurrentDatabase(db *sql.DB) (string, error) {
	var out string
	if err := db.QueryRow(`SELECT DATABASE()`).Scan(&out); err != nil {
		return "", err
	}
	return out, nil
}

func TableHasColumn(db *sql.DB, tableName string, column string) (bool, error) {
	var cnt int
	err := db.QueryRow(`
SELECT COUNT(1)
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?
  AND column_name = ?`,
		tableName,
		column,
	).Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func TableExists(db *sql.DB, tableName string) (bool, error) {
	var cnt int
	err := db.QueryRow(`
SELECT COUNT(1)
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name = ?`,
		tableName,
	).Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func ListKlineTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
SELECT table_name
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name LIKE 'future_kline_%'
ORDER BY table_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
