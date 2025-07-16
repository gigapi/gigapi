package repository

import (
	"database/sql"
	"fmt"
)



// TODO: Implement the incremental initialization of TablesTable

func CreateDuckDBTablesTable(db *sql.DB) error {
	// Adjusted schema using DuckDB's ARRAY type
	query := `
	CREATE TABLE IF NOT EXISTS tables (
		name VARCHAR PRIMARY KEY, 
		path VARCHAR,
		field_names  VARCHAR[],     
		field_types VARCHAR[],     
		order_by VARCHAR[],       
		engine VARCHAR,     
		timestamp_field VARCHAR,    
		timestamp_precision VARCHAR,
		partition_by VARCHAR,
		auto_timestamp BOOLEAN DEFAULT FALSE,
	);
	`

	// Execute the query to create the table if it doesn't exist
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create 'tables' table in DuckDB: %v", err)
	}

	return nil
}

/*func InsertTableMetadata(db *sql.DB, table *shared.Table) error {
	orderByJSON, err := json.Marshal(table.OrderBy)
	if err != nil {
		return err
	}

	query := `INSERT INTO tables (
        name, path, order_by, engine, timestamp_field, timestamp_precision, partition_by, auto_timestamp
    ) SELECT ?, ?, ?::JSON::VARCHAR[], ?, ?, ?, ?, ? ON CONFLICT DO NOTHING`
	_, err = db.Exec(query,
		table.Name, table.Path, string(orderByJSON),
		table.Engine, table.TimestampField, table.TimestampPrecision, table.PartitionBy, table.AutoTimestamp)

	return err
}

func GetAllTableMetadata(db *sql.DB) ([]*shared.Table, error) {
	query := `SELECT name, path, order_by, engine, timestamp_field, timestamp_precision, partition_by, auto_timestamp FROM tables;`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := make([]*shared.Table, 0)
	for rows.Next() {
		var table shared.Table
		var orderBy []any
		err := rows.Scan(&table.Name, &table.Path, &orderBy, &table.Engine, &table.TimestampField,
			&table.TimestampPrecision, &table.PartitionBy, &table.AutoTimestamp)
		if err != nil {
			return nil, err
		}
		for _, v := range orderBy {
			table.OrderBy = append(table.OrderBy, v.(string))
		}
		tables = append(tables, &table)
	}
	return tables, nil
}*/
