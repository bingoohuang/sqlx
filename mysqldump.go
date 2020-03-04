package sqlmore

// from https://github.com/JamesStewy/go-mysqldump

import (
	"database/sql"
	"errors"
	"io"
	"strings"
	"text/template"
	"time"
)

const version = "0.2.2"

const headerTmpl = `-- Go SQL Dump {{ .DumpVersion }}
--
-- ------------------------------------------------------
-- Server version	{{ .ServerVersion }}
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
`

const createTableTmpl = `
--
-- Table structure for table {{ .Name }}
--
DROP TABLE IF EXISTS {{ .Name }};
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
{{ .SQL }};
/*!40101 SET character_set_client = @saved_cs_client */;`

const tableDataTmplStart = `
--
-- Dumping data for table {{ .Name }}
--
LOCK TABLES {{ .Name }} WRITE;
/*!40000 ALTER TABLE {{ .Name }} DISABLE KEYS */;

INSERT INTO {{ .Name }} VALUES `
const tableDataTmplEnd = `;

/*!40000 ALTER TABLE {{ .Name }} ENABLE KEYS */;
UNLOCK TABLES;

`
const tailTmpl = `-- Dump completed on {{ .CompleteTime }} `

type mySQLDump struct {
	db *sql.DB
}

// MySQLDump creates a MYSQL Dump based on the options supplied through the dumper.
func MySQLDump(db *sql.DB, writer io.Writer) error {
	m := &mySQLDump{db: db}

	// UrlGet server version
	serverVersion, err := m.getServerVersion()
	if err != nil {
		return err
	}

	t, err := template.New("mysqldump_header").Parse(headerTmpl)
	if err != nil {
		return err
	}

	if err = t.Execute(writer, struct{ DumpVersion, ServerVersion string }{
		DumpVersion: version, ServerVersion: serverVersion}); err != nil {
		return err
	}

	// UrlGet tables
	tables, err := m.getTables()
	if err != nil {
		return err
	}

	ct, _ := template.New("mysqldump_createTable").Parse(createTableTmpl)
	ds, _ := template.New("mysqldump_tableDataStart").Parse(tableDataTmplStart)
	de, _ := template.New("mysqldump_tableDataEnd").Parse(tableDataTmplEnd)

	// UrlGet sql for each table
	for _, name := range tables {
		if err := m.createTable(ct, ds, de, writer, name); err != nil {
			return err
		}
	}

	// Write MySQLDump to file
	t, err = template.New("mysqldump-tail").Parse(tailTmpl)
	if err != nil {
		return err
	}

	return t.Execute(writer, struct{ CompleteTime string }{CompleteTime: time.Now().String()})
}

func (m *mySQLDump) getTables() ([]string, error) {
	tables := make([]string, 0)

	// UrlGet table list
	rows, err := m.db.Query("SHOW TABLES")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}

		tables = append(tables, table.String)
	}

	return tables, rows.Err()
}

func (m *mySQLDump) getServerVersion() (string, error) {
	var serverVersion sql.NullString
	if err := m.db.QueryRow("SELECT version()").Scan(&serverVersion); err != nil {
		return "", err
	}

	return serverVersion.String, nil
}

func (m *mySQLDump) createTable(ct, ds, de *template.Template, writer io.Writer, name string) error {
	sql, err := m.createTableSQL(name)
	if err != nil {
		return err
	}

	if err = ct.Execute(writer, struct{ Name, SQL string }{Name: "`" + name + "`", SQL: sql}); err != nil {
		return err
	}

	return m.createTableValues(ds, de, writer, name)
}

func (m *mySQLDump) createTableSQL(name string) (string, error) {
	var tableReturn sql.NullString

	var tableSQL sql.NullString

	err := m.db.QueryRow("SHOW CREATE TABLE "+name).Scan(&tableReturn, &tableSQL)

	if err != nil {
		return "", err
	}

	if tableReturn.String != name {
		return "", errors.New("returned table is not the same as requested table")
	}

	return tableSQL.String, nil
}

// nolint funlen
func (m *mySQLDump) createTableValues(ds, de *template.Template, writer io.Writer, name string) error {
	// #nosec G202
	rows, err := m.db.Query("SELECT * FROM " + name)
	if err != nil {
		return err
	}

	defer rows.Close()

	// UrlGet columns
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if len(columns) == 0 {
		return errors.New("no columns in table " + name + ".")
	}

	rowsIndex := 0

	for rows.Next() {
		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))

		for i := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		if rowsIndex == 0 {
			if err = ds.Execute(writer, struct{ Name string }{Name: "`" + name + "`"}); err != nil {
				return err
			}
		}

		rowsIndex++

		dataStrings := make([]string, len(columns))

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = "'" + strings.Replace(value.String, "'", "''", -1) + "'"
			} else {
				dataStrings[key] = "null"
			}
		}

		if rowsIndex > 1 {
			_, _ = writer.Write([]byte(","))
		}

		_, _ = writer.Write([]byte("(" + strings.Join(dataStrings, ",") + ")"))
	}

	if rowsIndex > 0 {
		if err = de.Execute(writer, struct{ Name string }{Name: "`" + name + "`"}); err != nil {
			return err
		}
	}

	return rows.Err()
}
