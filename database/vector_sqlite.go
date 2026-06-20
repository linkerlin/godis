//go:build sqlite_backend

package database

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func initSQLiteVectorSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS vector_set_meta (
			set_key TEXT PRIMARY KEY,
			dim INTEGER NOT NULL,
			vec_table TEXT NOT NULL UNIQUE
		)`,
		`CREATE TABLE IF NOT EXISTS vector_item_meta (
			set_key TEXT NOT NULL,
			item_id TEXT NOT NULL,
			rowid INTEGER NOT NULL,
			metadata_json TEXT NOT NULL DEFAULT '{}',
			values_json TEXT NOT NULL,
			PRIMARY KEY (set_key, item_id)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("init vector schema: %w", err)
		}
	}
	_, _ = db.Exec(`ALTER TABLE vector_item_meta ADD COLUMN values_json TEXT NOT NULL DEFAULT '[]'`)
	return nil
}

func vectorTableName(setKey string) string {
	sum := sha256.Sum256([]byte(setKey))
	return fmt.Sprintf("vec_%x", sum[:8])
}

func ensureVectorSetTable(db *sql.DB, setKey string, dim int) (string, error) {
	var existingDim int
	var table string
	err := db.QueryRow(
		`SELECT dim, vec_table FROM vector_set_meta WHERE set_key = ?`, setKey,
	).Scan(&existingDim, &table)
	switch {
	case err == sql.ErrNoRows:
		table = vectorTableName(setKey)
		ddl := fmt.Sprintf(
			`CREATE VIRTUAL TABLE %s USING vec0(embedding float[%d])`,
			table, dim,
		)
		if _, err := db.Exec(ddl); err != nil {
			return "", err
		}
		_, err = db.Exec(
			`INSERT INTO vector_set_meta(set_key, dim, vec_table) VALUES (?, ?, ?)`,
			setKey, dim, table,
		)
		return table, err
	case err != nil:
		return "", err
	case existingDim != dim:
		return "", fmt.Errorf("dimension mismatch")
	default:
		return table, nil
	}
}

func formatVectorLiteral(values []float64) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = fmt.Sprintf("%g", v)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func parseVectorLiteral(raw string) ([]float64, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return nil, fmt.Errorf("invalid vector literal")
	}
	raw = raw[1 : len(raw)-1]
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("empty vector literal")
	}
	parts := strings.Split(raw, ",")
	out := make([]float64, 0, len(parts))
	for _, part := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func sqliteVSAddVector(db *sql.DB, setKey, itemID string, values []float64, metadata map[string]string) (bool, error) {
	if len(values) == 0 {
		return false, fmt.Errorf("empty vector")
	}
	table, err := ensureVectorSetTable(db, setKey, len(values))
	if err != nil {
		return false, err
	}

	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return false, err
	}
	valuesJSON, err := json.Marshal(values)
	if err != nil {
		return false, err
	}
	vecLiteral := formatVectorLiteral(values)

	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var existingRowID sql.NullInt64
	err = tx.QueryRow(
		`SELECT rowid FROM vector_item_meta WHERE set_key = ? AND item_id = ?`,
		setKey, itemID,
	).Scan(&existingRowID)
	isNew := err == sql.ErrNoRows
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	if existingRowID.Valid {
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE rowid = ?`, table), existingRowID.Int64); err != nil {
			return false, err
		}
	}

	res, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(embedding) VALUES (?)`, table), vecLiteral)
	if err != nil {
		return false, err
	}
	rowID, err := res.LastInsertId()
	if err != nil {
		return false, err
	}

	if existingRowID.Valid {
		_, err = tx.Exec(
			`UPDATE vector_item_meta SET rowid = ?, metadata_json = ?, values_json = ? WHERE set_key = ? AND item_id = ?`,
			rowID, string(metaJSON), string(valuesJSON), setKey, itemID,
		)
	} else {
		_, err = tx.Exec(
			`INSERT INTO vector_item_meta(set_key, item_id, rowid, metadata_json, values_json) VALUES (?, ?, ?, ?, ?)`,
			setKey, itemID, rowID, string(metaJSON), string(valuesJSON),
		)
	}
	if err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return isNew, nil
}

type sqliteVectorHit struct {
	id       string
	score    float64
	values   []float64
}

func sqliteVSSearchVectors(db *sql.DB, setKey string, query []float64, k int) ([]sqliteVectorHit, error) {
	if k <= 0 {
		k = 10
	}
	var table string
	err := db.QueryRow(`SELECT vec_table FROM vector_set_meta WHERE set_key = ?`, setKey).Scan(&table)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	vecLiteral := formatVectorLiteral(query)
	rows, err := db.Query(fmt.Sprintf(`
		SELECT m.item_id, v.distance, m.values_json
		FROM %s v
		JOIN vector_item_meta m ON m.rowid = v.rowid AND m.set_key = ?
		WHERE v.embedding MATCH ?
		  AND k = %d
		ORDER BY v.distance`, table, k), setKey, vecLiteral)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hits []sqliteVectorHit
	for rows.Next() {
		var id, valuesJSON string
		var distance float64
		if err := rows.Scan(&id, &distance, &valuesJSON); err != nil {
			return nil, err
		}
		var values []float64
		if err := json.Unmarshal([]byte(valuesJSON), &values); err != nil {
			return nil, err
		}
		hits = append(hits, sqliteVectorHit{
			id:     id,
			score:  1.0 / (1.0 + distance),
			values: values,
		})
	}
	return hits, rows.Err()
}
