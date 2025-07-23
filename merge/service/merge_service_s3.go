package service

import (
	"context"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/gigapi/metadata"
	"github.com/jmoiron/sqlx"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type s3Desc struct {
	layer     config.LayersConfiguration
	hostname  string
	bucket    string
	path      string
	apiKey    string
	apiSecret string
	secure    bool
	urlStyle  string
}

func parseS3Url(layer config.LayersConfiguration) (s3Desc, error) {
	res := s3Desc{layer: layer}
	s3Url, err := url.Parse(layer.URL)
	if err != nil {
		return res, err
	}

	if s3Url.Scheme != "s3" {
		return res, fmt.Errorf("unsupported S3 URL scheme: %q", s3Url.Scheme)
	}

	res.hostname = s3Url.Host
	pathParts := strings.SplitN(strings.TrimPrefix(s3Url.Path, "/"), "/", 2)
	res.bucket = pathParts[0]
	if len(pathParts) > 1 && pathParts[1] != "" {
		res.path = pathParts[1]
	}
	res.apiKey = layer.Auth.Key
	res.apiSecret = layer.Auth.Secret
	res.secure = true
	if s3Url.Query().Get("secure") == "false" {
		res.secure = false
	}
	res.urlStyle = "vhost"
	if s3Url.Query().Get("url-style") == "path" {
		res.urlStyle = "path"
	}
	return res, nil
}

type s3MergeServicePerformer struct {
	table   *shared.Table
	tmpPath string

	desc s3Desc
}

func newS3MergeService(layer config.LayersConfiguration, table *shared.Table) (mergeService, error) {
	tmpPath, err := buildPath(layer, table, "tmp")
	if err != nil {
		return nil, err
	}
	dataPath, err := buildPath(layer, table, "data")
	if err != nil {
		return nil, err
	}

	s3D, err := parseS3Url(layer)
	if err != nil {
		return nil, err
	}

	performer := &s3MergeServicePerformer{
		table:   table,
		tmpPath: os.TempDir(),
		desc:    s3D,
	}

	savePerf, err := newS3SavePerformer(layer, table)
	if err != nil {
		return nil, err
	}

	manager := &mergeServiceManager{
		dataPath:              dataPath,
		tmpPath:               tmpPath,
		table:                 table,
		index:                 table.Index,
		mergeServicePerformer: performer,
		savePerformer:         savePerf,
	}
	return manager, nil
}

func (s *s3MergeServicePerformer) getPaths(files []string) []string {
	var res []string
	path := strings.Trim(s.desc.path, "/")
	if path != "" {
		path += "/"
	}
	for _, f := range files {
		res = append(res, fmt.Sprintf("s3://%s/%s%s/%s/%s",
			s.desc.bucket,
			path,
			s.table.Database,
			s.table.Name,
			f))
	}
	return res
}

func (s *s3MergeServicePerformer) createSecret(conn *sqlx.Conn) (func(), error) {
	sanitizedLName := s.desc.layer.Name
	sanitizedLName = regexp.MustCompile("[^a-zA-Z0-9_]").ReplaceAllString(sanitizedLName, "_")
	secretName := fmt.Sprintf("secret_%s", sanitizedLName)
	req := fmt.Sprintf(`CREATE OR REPLACE SECRET %s (
    TYPE s3,
    USE_SSL %t,
    KEY_ID %s,
    SECRET %s,
	ENDPOINT '%s',
	SCOPE 's3://%s',
    URL_STYLE '%s'
);`, secretName, s.desc.secure, s.desc.apiKey, s.desc.apiSecret, s.desc.hostname, s.desc.bucket, s.desc.urlStyle)
	_, err := conn.ExecContext(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return func() { conn.ExecContext(context.Background(), "DROP SECRET IF EXISTS %s;", secretName) }, nil
}

func (s *s3MergeServicePerformer) mergeFirstIteration(p metadata.MergePlan) error {
	conn, cancel, err := utils.ConnectDuckDB("")
	if err != nil {
		return err
	}
	defer cancel()

	destroySecret, err := s.createSecret(conn)
	if err != nil {
		return err
	}
	defer destroySecret()

	req := fmt.Sprintf(
		`COPY(FROM read_parquet(ARRAY['%s'], hive_partitioning = false, union_by_name = true) ORDER BY %s)TO '%s' (FORMAT 'parquet')`,
		strings.Join(s.getPaths(p.From), "','"),
		strings.Join(s.table.OrderBy, " ASC,")+" ASC",
		s.getPaths([]string{p.To})[0])
	_, err = conn.ExecContext(context.Background(), req)
	if err != nil {
		fmt.Println(req)
		fmt.Println("Error read_parquet_mergetree: ", err)
	}
	return err
}

func (s *s3MergeServicePerformer) mergeMany(p metadata.MergePlan) error {
	conn, cancel, err := utils.ConnectDuckDB("")
	if err != nil {
		return err
	}
	defer cancel()

	destroySecret, err := s.createSecret(conn)
	if err != nil {
		return err
	}
	defer destroySecret()

	err = installChSql(conn)
	if err != nil {
		return err
	}

	createTableSQL := fmt.Sprintf(
		`COPY(SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s'))TO '%s' (FORMAT 'parquet')`,
		strings.Join(s.getPaths(p.From), "','"),
		strings.Join(s.table.OrderBy, ","),
		s.getPaths([]string{p.To})[0])
	_, err = conn.ExecContext(context.Background(), createTableSQL)

	if err != nil {
		fmt.Println(createTableSQL)
		fmt.Println("Error read_parquet_mergetree: ", err)
	}
	return err
}

func (s *s3MergeServicePerformer) mergeOne(p metadata.MergePlan) error {
	// Initialize MinIO client
	minioClient, err := minio.New(s.desc.hostname, &minio.Options{
		Creds:  credentials.NewStaticV4(s.desc.apiKey, s.desc.apiSecret, ""),
		Secure: s.desc.secure, // Set to false if you're not using HTTPS
	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	parts := strings.Split(p.From[0], "/")
	filename := parts[len(parts)-1]

	path := s.desc.path
	if path != "" {
		path += "/"
	}
	// Download the file from S3 to tmp folder
	sourceKey := fmt.Sprintf("%s%s/%s/%s", path, s.table.Database, s.table.Name, p.From[0])
	tmpFile := filepath.Join(s.tmpPath, filename)

	err = minioClient.FGetObject(context.Background(), s.desc.bucket, sourceKey, tmpFile, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}
	defer os.Remove(tmpFile) // Clean up the temporary file

	// Upload the file from tmp folder to S3
	destinationKey := fmt.Sprintf("%s%s/%s/%s", path, s.table.Database, s.table.Name, p.To)
	_, err = minioClient.FPutObject(
		context.Background(), s.desc.bucket, destinationKey, tmpFile, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %w", err)
	}

	return nil
}
