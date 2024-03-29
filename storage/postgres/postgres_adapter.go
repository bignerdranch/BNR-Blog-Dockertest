package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq" // Imported as a side-effect to register drivers with database/sql package

	"BNR-Blog-Dockertest/storage"
)

const (
	sslModeKey  = "sslmode"
	passwordKey = "password"
	hostKey     = "host"
	portKey     = "port"
	userKey     = "user"
	dbNameKey   = "dbname"
)

// PgAdapter represents the postgres storage adapter
type PgAdapter struct {
	tableName string
	conn      *sql.DB
}

// createTable creates a new table based on constant values
func (a PgAdapter) createTable(tableName string) error {
	sqlStatement := fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY,number TEXT);", tableName)

	_, err := a.conn.Exec(sqlStatement)
	if err != nil {
		return err
	}

	return nil
}

// PgOptions describes connection options; exported for use in other packages that need initialization options
type PgOptions struct {
	password  string
	tableName string
	sslMode   string
	host      string
	port      string
	userName  string
	dbName    string
}

// PgOptionFunc describes functions which add optional connection variables to Postgres
type PgOptionFunc func(options *PgOptions)

// WithPassword is an optional function to provide a password to connect to the database with; default is empty
func WithPassword(password string) PgOptionFunc {
	return func(options *PgOptions) {
		options.password = password
	}
}

//WithTableName is an optional function to provide a table name for phone number operations; default is using the database name
func WithTableName(tableName string) PgOptionFunc {
	return func(options *PgOptions) {
		options.tableName = tableName
	}
}

// applyOpts iterates over the options provided, adds them to the connection variables map, and returns the connection options
// in string format as optKey=optValue
func applyOpts(connVars *PgOptions, pgOpts []PgOptionFunc) string {
	for _, pgOpt := range pgOpts {
		pgOpt(connVars)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "%s=%s %s=%s %s=%s %s=%s %s=%s", hostKey, connVars.host, portKey, connVars.port, dbNameKey,
		connVars.dbName, userKey, connVars.userName, sslModeKey, connVars.sslMode) // Apply provided options
	if connVars.password != "" { // Add to connection string password if provided
		fmt.Fprintf(&sb, " %s=%s", passwordKey, connVars.password)
	}

	return sb.String()
}

// NewAdapter instantiates a new postgres PgAdapter
func NewAdapter(host string, port string, user string, dbName string, pgOpts ...PgOptionFunc) (*PgAdapter, error) {
	connVars := &PgOptions{host: host, port: port, dbName: dbName, userName: user, sslMode: "disable"}
	psqlInfo := applyOpts(connVars, pgOpts)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	tableName := dbName           // Default table name to create to db name
	if connVars.tableName != "" { // If option passed in for table name, use the option instead of default
		tableName = connVars.tableName
	}

	adapter := &PgAdapter{conn: db, tableName: tableName}
	_ = adapter.createTable(tableName) // If table already created, ignore error

	return adapter, nil
}

// CreatePhoneNumber creates a phone number to insert into the database returning the id of the inserted item
func (a PgAdapter) CreatePhoneNumber(number string) (int, error) {
	sqlStatement := fmt.Sprintf("INSERT INTO %s (number)	VALUES ($1) RETURNING id", a.tableName)

	row := a.conn.QueryRow(sqlStatement, number)

	var id int
	err := row.Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// UpdatePhoneNumber updates a phone number based on the data passed in; error if number cannot be updated
func (a PgAdapter) UpdatePhoneNumber(number storage.PhoneNumber) error {
	sqlStatement := fmt.Sprintf("UPDATE %s SET number=$1 WHERE id=$2", a.tableName)

	_, err := a.conn.Exec(sqlStatement, number.Number, number.ID)
	if err != nil {
		return err
	}

	return nil
}

// GetPhoneNumbers retrieves a list of phone numbers from the database; error if list cannot be retrieved
func (a PgAdapter) GetPhoneNumbers() ([]storage.PhoneNumber, error) {
	sqlStatement := fmt.Sprintf("SELECT * FROM %s", a.tableName)

	rows, err := a.conn.Query(sqlStatement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var numbers []storage.PhoneNumber
	var number storage.PhoneNumber
	for rows.Next() {
		err = rows.Scan(&number.ID, &number.Number)
		if err != nil {
			return nil, fmt.Errorf("could not transform rows into phone numbers: %v", err)
		}

		numbers = append(numbers, number)
	}
	// Get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error while iterating over rows: %v", err)
	}

	return numbers, nil
}

// RemovePhoneNumber deletes a number from the database based on id
func (a PgAdapter) RemovePhoneNumber(id int) error {
	sqlStatement := fmt.Sprintf("DELETE FROM %s WHERE id=$1", a.tableName)

	_, err := a.conn.Exec(sqlStatement, id)
	if err != nil {
		return err
	}

	return nil
}

// insertNumbers is a test helper function for inserting phone numbers
func (a PgAdapter) insertNumbers(numbers []storage.PhoneNumber) error {
	sqlStatement := fmt.Sprintf("INSERT INTO %s (number)	VALUES ($1)", a.tableName)

	for _, number := range numbers {
		_, err := a.conn.Exec(sqlStatement, number.Number)
		if err != nil {
			return err
		}
	}

	return nil
}
