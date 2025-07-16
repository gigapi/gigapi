package repository

import (
	"fmt"
)

func ExecuteQuery(query string) error {
	db := GetDB()
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	return nil
}
