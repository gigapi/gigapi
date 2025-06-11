package service

import (
	"context"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/metadata"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
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

	if m.layer.Type == "fs" && layerTo.Name == "" {
		err = m.doRemoveFs(plan)
	} else if m.layer.Type == "fs" && layerTo.Type == "fs" {
		err = m.doMoveFs2Fs(plan, layerTo)
	} else {
		err = fmt.Errorf("unsupported move from %s to %s", m.layer.Type, layerTo.Type)
	}
	if err != nil {
		return false, err
	}

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

	entryFrom := m.t.Index.Get(plan.LayerFrom, plan.PathFrom)
	if entryFrom == nil {
		return nil
	}

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

	entryTo := *entryFrom
	entryTo.Path = plan.PathTo
	entryTo.Layer = plan.LayerTo
	_, err = m.t.Index.Batch([]*metadata.IndexEntry{&entryTo}, []*metadata.IndexEntry{entryFrom}).Get()
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
