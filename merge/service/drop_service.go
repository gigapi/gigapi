package service

import (
	"context"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/metadata"
	"os"
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
	if d.layer.Type == "fs" {
		err = d.dropOneFs(plan)
	} else {
		err = fmt.Errorf("unsupported drop from %s", d.layer.Type)
	}
	if err != nil {
		return false, err
	}
	_, err = d.t.Index.GetDropPlanner().RmFromDropQueue(plan).Get()
	return err == nil, err
}

func (d *dropService) dropOneFs(plan metadata.DropPlan) error {
	path := buildPath(d.layer, d.t, "data")
	os.Remove(path)
	return nil
}
