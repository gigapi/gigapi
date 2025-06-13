package service

import (
	"context"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/metadata"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"os"
	"path/filepath"
	"time"
)

type dropService struct {
	database string
	table    string
	layer    config.LayersConfiguration
	ctx      context.Context
	cancel   context.CancelFunc
	t        *shared.Table
	writerId string

	s3Desc s3Desc
	fsPath string
}

func newDropService(layer config.LayersConfiguration, t *shared.Table) (*dropService, error) {
	var err error
	d := &dropService{
		database: t.Database,
		table:    t.Name,
		layer:    layer,
		ctx:      context.Background(),
		t:        t,
		writerId: "", /*fmt.Sprintf("drop_service_%s_%s", t.Database, t.Name)*/
	}

	switch layer.Type {
	case "fs":
		d.fsPath, err = buildPath(layer, t, "data")
		if err != nil {
			return nil, err
		}
	case "s3":
		d.s3Desc, err = parseS3Url(layer)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported layer type: %q", layer.Type)
	}
	return d, nil
}

func (d *dropService) Run() {
	d.ctx, d.cancel = context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-d.ctx.Done():
				return
			case <-time.After(time.Second * 10):
				err := d.dropIteration()
				if err != nil {
					fmt.Printf("Error during drop iteration: %v\n", err)
				}
			}
		}
	}()
}

func (d *dropService) Stop() {
	d.cancel()
}

func (d *dropService) dropIteration() error {
	var ok bool
	var err error
	i := 0
	for true {
		ok, err = d.dropOne()
		if err != nil {
			fmt.Println(err)
		}
		if !ok {
			return nil
		}
		i++
	}
	return err
}

func (d *dropService) dropOne() (bool, error) {
	plan, err := d.t.Index.GetDropPlanner().GetDropQueue(d.writerId, d.layer.Name)
	if err != nil {
		return false, err
	}
	if plan.Path == "" {
		return false, nil
	}
	switch d.layer.Type {
	case "fs":
		err = d.dropOneFs(plan)
	case "s3":
		err = d.dropOneS3(plan)
	default:
		err = fmt.Errorf("unsupported drop from %s", d.layer.Type)
	}
	if err != nil {
		return false, err
	}
	_, err = d.t.Index.GetDropPlanner().RmFromDropQueue(plan).Get()
	return err == nil, err
}

func (d *dropService) dropOneFs(plan metadata.DropPlan) error {
	path := filepath.Join(d.fsPath, plan.Path)
	os.Remove(path)
	return nil
}

func (d *dropService) dropOneS3(plan metadata.DropPlan) error {
	minioClient, err := minio.New(d.s3Desc.hostname, &minio.Options{
		Creds:  credentials.NewStaticV4(d.s3Desc.apiKey, d.s3Desc.apiSecret, ""),
		Secure: d.s3Desc.secure, // Set to false if you're not using HTTPS

	})
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	path := d.s3Desc.path
	if path != "" {
		path = filepath.Join(path, plan.Path)
	}
	dropKey := fmt.Sprintf("%s%s/%s/%s", path, d.database, d.table, plan.Path)
	err = minioClient.RemoveObject(context.Background(), d.s3Desc.bucket, dropKey, minio.RemoveObjectOptions{})
	if err != nil {
		fmt.Println("s3 rm error: ", err)
	}
	return nil
}
