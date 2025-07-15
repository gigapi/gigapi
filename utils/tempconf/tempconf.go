package tempconf

import (
	"fmt"
	"os"
	"slices"
	"strings"
)

//TODO: move this to the config repo

func GetPostgresConnectionString() string {
	if os.Getenv("POSTGRES_HOST") == "" {
		return ""
	}
	sslMode := "sslmode=disable"
	if slices.Contains([]string{"yes", "y", "1", "true"}, strings.ToLower(os.Getenv("POSTGRES_SSL"))) {
		sslMode = "sslmode=enable"
	}
	return fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?%s",
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_PORT"),
		os.Getenv("POSTGRES_DB"),
		sslMode)
}

func GetDuckDBAttachString() string {
	return fmt.Sprintf("ducklake:postgres:dbname=%s user=%s password=%s host=%s port=%s",
		os.Getenv("POSTGRES_DB"),
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_PORT"))
}
