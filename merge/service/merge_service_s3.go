package service

import (
	"context"
	"fmt"
	"github.com/gigapi/gigapi/v2/merge/utils"
	"github.com/gigapi/metadata"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/sync/errgroup"
	"os"
	"path/filepath"
	"strings"
)

type s3MergeService struct {
	fsMergeService
	s3Config
	tmpPath string
}

func (s *s3MergeService) GetFilesToMerge(iteration int) ([]FileDesc, error) {
	minioClient, err := minio.New(s.url, &minio.Options{
		Creds:  credentials.NewStaticV4(s.key, s.secret, ""),
		Secure: s.secure,
		Region: s.region,
	})
	if err != nil {
		return nil, err
	}
	var res []FileDesc
	snapshot := ""
	c := minioClient.ListObjects(context.Background(), s.bucket,
		minio.ListObjectsOptions{
			Prefix:     s.s3Config.path + "/",
			MaxKeys:    1000,
			StartAfter: snapshot,
		})
	snapshot = "a"
	suffix := fmt.Sprintf("%d.parquet", iteration)
	for snapshot != "" {
		snapshot = ""
		for obj := range c {
			if !strings.HasSuffix(obj.Key, suffix) {
				continue
			}
			res = append(res, FileDesc{
				name: obj.Key,
				size: obj.Size,
			})
			snapshot = obj.Key
		}
		if snapshot == "" {
			break
		}
		c = minioClient.ListObjects(context.Background(), s.bucket,
			minio.ListObjectsOptions{
				Prefix:     s.s3Config.path,
				MaxKeys:    1000,
				StartAfter: snapshot,
			})
	}
	return res, nil
}

func escapeString(s string) string {
	return strings.Replace(s, "'", "''", -1)
}

func (s *s3MergeService) merge(p metadata.MergePlan) error {
	// Create a temporary merged file
	tmpFilePath := filepath.Join(s.tmpPath, p.To)

	conn, cancel, err := utils.ConnectDuckDB("?access_mode=READ_WRITE&allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	defer cancel()
	_, err = conn.Exec("INSTALL chsql FROM community")
	if err != nil {
		fmt.Println("Error loading chsql extension: ", err)
		return err
	}
	_, err = conn.Exec("LOAD chsql")
	if err != nil {
		fmt.Println("Error loading chsql extension: ", err)
		return err
	}
	defer conn.Close()

	from := make([]string, len(p.From))
	for i, file := range p.From {
		from[i] = fmt.Sprintf("s3://%s/%s", s.bucket, file)
	}

	createSecret := fmt.Sprintf(`CREATE SECRET (
  TYPE S3, 
  KEY_ID '%s', 
  SECRET '%s', 
  ENDPOINT '%s', 
  USE_SSL %t, 
  URL_STYLE 'path'
);`, escapeString(s.key), escapeString(s.secret), escapeString(s.url), s.secure)
	fmt.Println(createSecret)

	_, err = conn.Exec(createSecret)
	if err != nil {
		return err
	}
	createTableSQL := fmt.Sprintf(
		`COPY(SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s'))TO '%s' (FORMAT 'parquet')`,
		strings.Join(from, "','"),
		strings.Join(s.table.OrderBy, ","), tmpFilePath)
	fmt.Println(createTableSQL)
	_, err = conn.Exec(createTableSQL)
	if err != nil {
		fmt.Println("Error read_parquet_mergetree: ", err)
		return err
	}
	defer os.Remove(tmpFilePath)

	saveSvc := s3SaveService{
		fsSaveService: fsSaveService{},
		s3Config:      s.s3Config,
	}

	err = saveSvc.uploadToS3(tmpFilePath)
	if err != nil {
		return err
	}

	minioClient, err := saveSvc.createMinioClient()
	eg := errgroup.Group{}
	for _, f := range p.From {
		_f := f
		eg.Go(func() error {
			return minioClient.RemoveObject(context.Background(), s.bucket, _f, minio.RemoveObjectOptions{})
		})
	}
	return eg.Wait()
}

func (s *s3MergeService) DoMerge(merges []metadata.MergePlan) error {
	return s.doMerge(merges, s.merge)
}
