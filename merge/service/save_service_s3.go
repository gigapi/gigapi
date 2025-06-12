package service

import (
	"context"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"os"
	"strings"
)

type s3SavePerformer struct {
	layer config.LayersConfiguration
	t     *shared.Table

	desc s3Desc
}

func newS3SaveService(layer config.LayersConfiguration, table *shared.Table) (saveService, error) {
	desc, err := parseS3Url(layer.URL)
	if err != nil {
		return nil, err
	}
	return &saveServiceManager{
		table:   table,
		layer:   layer,
		tmpPath: os.TempDir(),
		savePerformer: &s3SavePerformer{
			layer: layer,
			t:     table,
			desc:  desc,
		},
	}, nil
}

func (s *s3SavePerformer) moveTmp(tmpPath string, filePath string) error {
	defer os.Remove(tmpPath)
	minioClient, err := minio.New(s.desc.hostname, &minio.Options{
		Creds:  credentials.NewStaticV4(s.desc.apiKey, s.desc.apiSecret, ""),
		Secure: s.desc.secure, // Set to false if you're not using HTTPS
	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}
	prefix := s.desc.path
	if prefix != "" {
		prefix += "/"
	}
	keyTo := fmt.Sprintf("%s%s/%s/%s", prefix, s.t.Database, s.t.Name, filePath)
	_, err = minioClient.FPutObject(context.Background(), s.desc.bucket, keyTo, tmpPath, minio.PutObjectOptions{})
	return err
}

func (s *s3SavePerformer) join(part ...string) string {
	return strings.Join(part, "/")
}

func (s *s3SavePerformer) base(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

func (s *s3SavePerformer) sizeB(path string) (int64, error) {
	minioClient, err := minio.New(s.desc.hostname, &minio.Options{
		Creds:  credentials.NewStaticV4(s.desc.apiKey, s.desc.apiSecret, ""),
		Secure: s.desc.secure, // Set to false if you're not using HTTPS
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create MinIO client: %w", err)
	}
	prefix := s.desc.path
	if prefix != "" {
		prefix += "/"
	}
	keyTo := fmt.Sprintf("%s%s/%s/%s", prefix, s.t.Database, s.t.Name, path)
	stat, err := minioClient.StatObject(context.Background(), s.desc.bucket, keyTo, minio.StatObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to get object size: %w", err)
	}
	return stat.Size, nil
}
