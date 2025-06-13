package service

import (
	"context"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/metadata"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

type moveService struct {
	database string
	table    string
	layer    config.LayersConfiguration
	ctx      context.Context
	cancel   context.CancelFunc
	t        *shared.Table
	writerId string
	tmpPath  string
}

func (m *moveService) Run() {
	m.ctx, m.cancel = context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(time.Second * 10):
				err := m.moveIteration()
				if err != nil {
					fmt.Printf("Error during move iteration: %v\n", err)
				}
			}
		}
	}()

}

func (m *moveService) Stop() {
	m.cancel()
}

func (m *moveService) moveIteration() error {
	i := 0
	for true {
		ok, err := m.moveOne()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		i++
	}
	return nil
}

func (m *moveService) moveOne() (bool, error) {
	plan, err := m.t.Index.GetMovePlanner().GetMovePlan(m.writerId, m.layer.Name)
	if err != nil {
		return false, err
	}
	if plan.PathFrom == "" {
		return false, nil
	}

	layerFromIdx := slices.IndexFunc(config.Config.Gigapi.Layers, func(configuration config.LayersConfiguration) bool {
		return configuration.Name == m.layer.Name
	})
	var layerTo config.LayersConfiguration
	if layerFromIdx < len(config.Config.Gigapi.Layers)-1 {
		layerTo = config.Config.Gigapi.Layers[layerFromIdx+1]
	}

	entryFrom := m.t.Index.Get(plan.LayerFrom, plan.PathFrom)
	if entryFrom == nil {
		m.t.Index.GetMovePlanner().EndMove(plan)
		return true, nil
	}

	switch m.layer.Type + layerTo.Type {
	case "fsfs":
		err = m.doMoveFs2Fs(plan, layerTo)
	case "fss3":
		err = m.doMoveFs2S3(plan, layerTo)
	case "s3fs":
		err = m.doMoveS32Fs(plan, layerTo)
	case "s3s3":
		err = m.doMoveS32S3(plan, layerTo)
	case "fs":
		err = m.doRemoveFs(plan)
	case "s3":
		err = m.doRemoveS3(plan)
	default:
		err = fmt.Errorf("unsupported move from %s to %s", m.layer.Type, layerTo.Type)
	}
	if err != nil {
		return false, err
	}

	entryTo := *entryFrom
	entryTo.Path = plan.PathTo
	entryTo.Layer = plan.LayerTo
	_, err = m.t.Index.Batch([]*metadata.IndexEntry{&entryTo}, []*metadata.IndexEntry{entryFrom}).Get()

	m.t.Index.GetMovePlanner().EndMove(plan)
	return true, nil
}

func (m *moveService) doMoveFs2Fs(plan metadata.MovePlan, layerTo config.LayersConfiguration) error {
	pathFrom, err := buildPath(m.layer, m.t, path.Join("data", plan.PathFrom))
	if err != nil {
		return err
	}
	pathTo, err := buildPath(layerTo, m.t, path.Join("data", plan.PathTo))
	if err != nil {
		return err
	}

	fmt.Printf("Moving %s to %s\n", pathFrom, pathTo)

	dirTo := filepath.Dir(pathTo)
	os.MkdirAll(dirTo, 0o755)
	f, err := os.Open(pathFrom)
	if err != nil {
		return err
	}
	defer f.Close()

	f2, err := os.Create(pathTo)
	if err != nil {
		return err
	}
	defer f2.Close()
	io.Copy(f2, f)
	return err
}

func (m *moveService) doMoveFs2S3(plan metadata.MovePlan, layerTo config.LayersConfiguration) error {
	desc, err := parseS3Url(layerTo)
	if err != nil {
		return err
	}

	minioClient, err := minio.New(desc.hostname, &minio.Options{
		Creds:  credentials.NewStaticV4(desc.apiKey, desc.apiSecret, ""),
		Secure: desc.secure,
	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	pathFrom, err := buildPath(m.layer, m.t, path.Join("data", plan.PathFrom))
	if err != nil {
		return err
	}
	path := desc.path
	if path != "" {
		path += "/"
	}
	keyTo := fmt.Sprintf("%s%s/%s/%s", path, m.database, m.table, plan.PathTo)

	_, err = minioClient.FPutObject(context.Background(), desc.bucket, keyTo, pathFrom, minio.PutObjectOptions{})
	return err
}

func (m *moveService) doMoveS32Fs(plan metadata.MovePlan, layerTo config.LayersConfiguration) error {
	desc, err := parseS3Url(m.layer)
	if err != nil {
		return err
	}

	minioClient, err := minio.New(desc.hostname, &minio.Options{
		Creds:  credentials.NewStaticV4(desc.apiKey, desc.apiSecret, ""),
		Secure: desc.secure,
	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	pathFrom := desc.path
	if pathFrom != "" {
		pathFrom += "/"
	}
	keyFrom := fmt.Sprintf("%s%s/%s/%s", pathFrom, m.database, m.table, plan.PathTo)

	toFilename := filepath.Base(plan.PathTo)
	tmpPath := filepath.Join(m.tmpPath, toFilename)
	pathTo, err := buildPath(layerTo, m.t, path.Join("data", plan.PathFrom))
	if err != nil {
		return err
	}

	err = minioClient.FGetObject(context.Background(), desc.bucket, keyFrom, tmpPath, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}

	err = os.Rename(tmpPath, pathTo)
	return err
}

func (m *moveService) doMoveS32S3(plan metadata.MovePlan, layerTo config.LayersConfiguration) error {
	descFrom, err := parseS3Url(m.layer)
	if err != nil {
		return err
	}

	minioClient, err := minio.New(descFrom.hostname, &minio.Options{
		Creds:  credentials.NewStaticV4(descFrom.apiKey, descFrom.apiSecret, ""),
		Secure: descFrom.secure,
	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	pathFrom := descFrom.path
	if pathFrom != "" {
		pathFrom += "/"
	}
	keyFrom := fmt.Sprintf("%s%s/%s/%s", pathFrom, m.database, m.table, plan.PathTo)

	toParts := strings.Split(layerTo.URL, "/")
	toFilename := toParts[len(toParts)-1]
	tmpPath := filepath.Join(m.tmpPath, toFilename)

	pathTo := descFrom.path
	if pathFrom != "" {
		pathTo += "/"
	}
	keyTo := fmt.Sprintf("%s%s/%s/%s", pathTo, m.database, m.table, plan.PathTo)
	if err != nil {
		return err
	}

	err = minioClient.FGetObject(context.Background(), descFrom.bucket, keyFrom, tmpPath, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}

	descTo, err := parseS3Url(layerTo)
	if err != nil {
		return err
	}
	minioClient, err = minio.New(descTo.hostname, &minio.Options{
		Creds:  credentials.NewStaticV4(descTo.apiKey, descTo.apiSecret, ""),
		Secure: descTo.secure,
	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}
	_, err = minioClient.FPutObject(context.Background(), descTo.bucket, keyTo, tmpPath, minio.PutObjectOptions{})
	return err
}

func (m *moveService) doRemoveFs(plan metadata.MovePlan) error {
	pathFrom, err := buildPath(m.layer, m.t, path.Join("data", plan.PathFrom))
	if err != nil {
		return err
	}
	fmt.Printf("Removing %s\n", pathFrom)
	return os.Remove(pathFrom)
}

func (m *moveService) doRemoveS3(plan metadata.MovePlan) error {
	descFrom, err := parseS3Url(m.layer)
	if err != nil {
		return err
	}

	minioClient, err := minio.New(descFrom.hostname, &minio.Options{
		Creds:  credentials.NewStaticV4(descFrom.apiKey, descFrom.apiSecret, ""),
		Secure: descFrom.secure,
	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	pathFrom := descFrom.path
	if pathFrom != "" {
		pathFrom += "/"
	}
	keyFrom := fmt.Sprintf("%s%s/%s/%s", pathFrom, m.database, m.table, plan.PathTo)

	err = minioClient.RemoveObject(context.Background(), descFrom.bucket, keyFrom, minio.RemoveObjectOptions{})
	return err
}
