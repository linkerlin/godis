//go:build sqlite_backend

package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

var (
	sqliteIndexDBOnce sync.Once
	sqliteIndexDB     *sql.DB
	sqliteIndexDBErr  error
)

func getSQLiteIndexDB() (*sql.DB, error) {
	sqliteIndexDBOnce.Do(func() {
		opts := sqliteIndexOptionsFromConfig()
		sqliteIndexDB, sqliteIndexDBErr = OpenSQLiteIndexDB(opts.Path, opts.MmapSize)
		if sqliteIndexDBErr != nil {
			return
		}
		sqliteIndexDBErr = initSQLiteSearchSchema(sqliteIndexDB)
		if sqliteIndexDBErr == nil {
			sqliteIndexDBErr = initSQLiteVectorSchema(sqliteIndexDB)
		}
	})
	return sqliteIndexDB, sqliteIndexDBErr
}

// resetSQLiteIndexDBForTest closes the shared index DB so tests can re-open another path.
func resetSQLiteIndexDBForTest() {
	if sqliteIndexDB != nil {
		_ = sqliteIndexDB.Close()
	}
	sqliteIndexDB = nil
	sqliteIndexDBErr = nil
	sqliteIndexDBOnce = sync.Once{}
}

func initSQLiteSearchSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS search_index_meta (
			name TEXT PRIMARY KEY,
			schema_json TEXT NOT NULL,
			created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
		)`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS search_fts_docs USING fts5(
			index_name UNINDEXED,
			doc_id UNINDEXED,
			body,
			fields_json UNINDEXED,
			tokenize='unicode61'
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("init search schema: %w", err)
		}
	}
	return nil
}

func sqliteIndexExists(db *sql.DB, indexName string) (bool, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(1) FROM search_index_meta WHERE name = ?`, indexName).Scan(&n)
	return n > 0, err
}

func sqliteFTCreateIndex(db *sql.DB, indexName string, textFields []string) error {
	exists, err := sqliteIndexExists(db, indexName)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("Index already exists")
	}
	schemaJSON, err := json.Marshal(textFields)
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO search_index_meta(name, schema_json) VALUES(?, ?)`, indexName, string(schemaJSON))
	return err
}

func sqliteFTDropIndex(db *sql.DB, indexName string) error {
	if _, err := db.Exec(`DELETE FROM search_fts_docs WHERE index_name = ?`, indexName); err != nil {
		return err
	}
	res, err := db.Exec(`DELETE FROM search_index_meta WHERE name = ?`, indexName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("Index '%s' does not exist", indexName)
	}
	return nil
}

func sqliteFTAddDocument(db *sql.DB, indexName, docID string, fields map[string]string) error {
	exists, err := sqliteIndexExists(db, indexName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("Index '%s' does not exist", indexName)
	}

	var parts []string
	for _, v := range fields {
		if v != "" {
			parts = append(parts, v)
		}
	}
	body := strings.Join(parts, " ")
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return err
	}

	_, err = db.Exec(`DELETE FROM search_fts_docs WHERE index_name = ? AND doc_id = ?`, indexName, docID)
	if err != nil {
		return err
	}
	_, err = db.Exec(
		`INSERT INTO search_fts_docs(index_name, doc_id, body, fields_json) VALUES(?, ?, ?, ?)`,
		indexName, docID, body, string(fieldsJSON),
	)
	return err
}

type sqliteSearchHit struct {
	docID   string
	fields  map[string]string
	score   float64
}

func sqliteFTSearchDocs(db *sql.DB, indexName, query string, limit, offset int) ([]sqliteSearchHit, int, error) {
	exists, err := sqliteIndexExists(db, indexName)
	if err != nil {
		return nil, 0, err
	}
	if !exists {
		return nil, 0, fmt.Errorf("Index '%s' does not exist", indexName)
	}
	if limit <= 0 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}

	matchQuery := strings.TrimSpace(query)
	if matchQuery == "" {
		matchQuery = "*"
	}

	countSQL := `SELECT COUNT(1) FROM search_fts_docs WHERE index_name = ?`
	countArgs := []any{indexName}
	if matchQuery != "*" {
		countSQL += ` AND search_fts_docs MATCH ?`
		countArgs = append(countArgs, matchQuery)
	}
	var total int
	if err := db.QueryRow(countSQL, countArgs...).Scan(&total); err != nil {
		return nil, 0, err
	}

	searchSQL := `SELECT doc_id, fields_json, bm25(search_fts_docs) AS score
		FROM search_fts_docs WHERE index_name = ?`
	searchArgs := []any{indexName}
	if matchQuery != "*" {
		searchSQL += ` AND search_fts_docs MATCH ?`
		searchArgs = append(searchArgs, matchQuery)
	}
	searchSQL += ` ORDER BY score LIMIT ? OFFSET ?`
	searchArgs = append(searchArgs, limit, offset)

	rows, err := db.Query(searchSQL, searchArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var hits []sqliteSearchHit
	for rows.Next() {
		var docID, fieldsJSON string
		var score float64
		if err := rows.Scan(&docID, &fieldsJSON, &score); err != nil {
			return nil, 0, err
		}
		fields := map[string]string{}
		_ = json.Unmarshal([]byte(fieldsJSON), &fields)
		hits = append(hits, sqliteSearchHit{docID: docID, fields: fields, score: -score})
	}
	return hits, total, rows.Err()
}

func parseSQLiteTextSchema(args [][]byte) (indexName string, fields []string, errMsg string) {
	if len(args) < 3 {
		return "", nil, "ERR wrong number of arguments for 'ft.create' command"
	}
	indexName = string(args[0])
	schemaStart := -1
	for i := 1; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "SCHEMA") {
			schemaStart = i + 1
			break
		}
	}
	if schemaStart < 0 || schemaStart >= len(args) {
		return "", nil, "ERR No schema specified"
	}
	for i := schemaStart; i < len(args); {
		fieldName := string(args[i])
		i++
		if i >= len(args) {
			return "", nil, fmt.Sprintf("ERR No type specified for field '%s'", fieldName)
		}
		fieldType := strings.ToUpper(string(args[i]))
		if fieldType != "TEXT" {
			return "", nil, fmt.Sprintf("ERR sqlite backend only supports TEXT fields, got %s for '%s'", fieldType, fieldName)
		}
		fields = append(fields, fieldName)
		i++
		for i < len(args) {
			if i+1 < len(args) && isFTFieldType(string(args[i+1])) {
				break
			}
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "SORTABLE", "NOINDEX", "NOSTEM":
				i++
			case "WEIGHT":
				i += 2
			default:
				i++
			}
		}
	}
	if len(fields) == 0 {
		return "", nil, "ERR No TEXT fields in schema"
	}
	return indexName, fields, ""
}

func parseSQLiteFTAddFields(args [][]byte) (indexName, docID string, fields map[string]string, errMsg string) {
	if len(args) < 4 {
		return "", "", nil, "ERR wrong number of arguments for 'ft.add' command"
	}
	indexName = string(args[0])
	docID = string(args[1])
	fieldsStart := 2
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "SCORE", "PAYLOAD", "LANGUAGE":
			i++
		case "NOSAVE":
		case "FIELDS":
			fieldsStart = i + 1
			i = len(args)
		default:
			fieldsStart = i
			i = len(args)
		}
	}
	if fieldsStart >= len(args) || (len(args)-fieldsStart)%2 != 0 {
		return "", "", nil, "ERR Fields must be specified as field-value pairs"
	}
	fields = make(map[string]string)
	for i := fieldsStart; i < len(args); i += 2 {
		fields[string(args[i])] = string(args[i+1])
	}
	return indexName, docID, fields, ""
}

func parseSQLiteFTSearchOptions(args [][]byte) (limit, offset int, withScores, noContent bool) {
	limit = 10
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NOCONTENT":
			noContent = true
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 < len(args) {
				offset, _ = strconv.Atoi(string(args[i+1]))
				limit, _ = strconv.Atoi(string(args[i+2]))
				i += 2
			}
		}
	}
	return limit, offset, withScores, noContent
}
