package service

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	parquet "github.com/fraugster/parquet-go"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi/v2/merge/metadata"
	"github.com/gigapi/gigapi/v2/merge/shared"
	"github.com/gigapi/gigapi/v2/merge/utils"
	metadata2 "github.com/gigapi/metadata"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

type DucklakeMergeService struct {
	lastMergeTime []time.Time
}

func (d *DucklakeMergeService) DoMerge() error {
	confs := getMergeConfigurations()
	if d.lastMergeTime == nil {
		d.lastMergeTime = make([]time.Time, len(confs))
		for i := range d.lastMergeTime {
			d.lastMergeTime[i] = time.Now()
		}
	}
	e := errgroup.Group{}
	for _, conf := range confs {
		_conf := conf
		if time.Since(d.lastMergeTime[conf.iteration()-1]) < time.Second*time.Duration(conf.timeoutS()) {
			continue
		}
		go func() {
			err := d.mergeIteration(_conf)
			if err != nil {
				fmt.Println(err)
				return
				//return fmt.Errorf("error during merge iteration: %w", err)
			}
			d.lastMergeTime[_conf.iteration()-1] = time.Now()
		}()
	}
	return e.Wait()
}

type ducklakePlan struct {
	metadata2.MergePlan
	From []metadata.FileDesc
}

func (d *DucklakeMergeService) mergeIteration(iteration mergeConfiguration) error {
	files, err := metadata.GetFiles("", int(iteration.iteration()), true)
	if err != nil {
		return err
	}
	filesMap := make(map[string]map[string][]metadata.FileDesc)
	unsortedTables := make(map[string]bool)
	for _, f := range files {
		if len(f.Table.OrderBy) == 0 {
			unsortedTables[f.Table.Name] = true
			continue
		}
		if _, ok := filesMap[f.Table.Name]; !ok {
			filesMap[f.Table.Name] = make(map[string][]metadata.FileDesc)
		}
		dir := path.Dir(f.Path)
		filesMap[f.Table.Name][dir] = append(filesMap[f.Table.Name][dir], f)
	}

	plans := make(map[string]map[string][]ducklakePlan)
	lastPlanSize := int64(0)
	incPlan := func(table string, dir string) {
		if _, ok := plans[table]; !ok {
			plans[table] = make(map[string][]ducklakePlan)
		}
		plans[table][dir] = append(plans[table][dir], ducklakePlan{
			MergePlan: metadata2.MergePlan{
				Table: table,
				To: path.Join(dir, fmt.Sprintf("%s.%d.parquet",
					uuid.New().String(),
					iteration.iteration()+1)),
				Iteration: int(iteration.iteration()),
			},
		})
		lastPlanSize = 0
	}

	for table, dirFiles := range filesMap {
		for dir, files := range dirFiles {
			incPlan(table, dir)
			for _, f := range files {
				if lastPlanSize+f.SizeBytes > iteration.maxResultBytes() || len(plans) == 0 {
					incPlan(table, dir)
				}
				plans[table][dir][len(plans[table][dir])-1].MergePlan.From = append(
					plans[table][dir][len(plans[table][dir])-1].MergePlan.From, f.Path)
				plans[table][dir][len(plans[table][dir])-1].From = append(
					plans[table][dir][len(plans[table][dir])-1].From, f)
			}
		}
	}

	for table, dirPlans := range plans {
		for _, plans := range dirPlans {
			performer := fsMergeServicePerformer{
				dataPath: path.Join(config.Config.Gigapi.Root, metadata.SCHEMA_NAME, table),
				tmpPath:  path.Join(config.Config.Gigapi.Root, "_tmp"),
				table: &shared.Table{
					OrderBy: []string{plans[0].From[0].Table.OrderBy[0]},
				},
			}
			for _, p := range plans {
				if len(p.From) == 0 {
					continue
				}
				if iteration.iteration() == 1 {
					err = performer.mergeFirstIteration(p.MergePlan)
				} else if len(p.From) == 1 {
					err = performer.mergeOne(p.MergePlan)
				} else {
					err = performer.mergeMany(p.MergePlan)
				}
				if err != nil {
					return err
				}
				toFileDesc, err := d.toFileDesc(&p)
				if err != nil {
					return err
				}
				fmt.Println(p.MergePlan.From)
				fmt.Println(p.To)
				fmt.Println(toFileDesc)
				err = metadata.FinishMerge(context.Background(), p.From, []metadata.FileDesc{toFileDesc},
					&p.From[0].Table)
				if err != nil {
					return err
				}
			}
		}
	}
	return err
}

func (d *DucklakeMergeService) toFileDesc(p *ducklakePlan) (metadata.FileDesc, error) {
	added := metadata.FileDesc{
		Id:                  0,
		Table:               p.From[0].Table,
		Path:                p.To,
		FooterSizeBytes:     0,
		RecordCount:         0,
		ColumnStats:         nil,
		FilePartitionValues: nil,
		PartitionId:         0,
	}
	if p.Iteration == 1 || len(p.From) > 1 {
		err := d.copyColumnIDs(
			path.Join(config.Config.Gigapi.Root, metadata.SCHEMA_NAME, p.Table, p.From[0].Path),
			path.Join(config.Config.Gigapi.Root, metadata.SCHEMA_NAME, p.Table, p.To))
		if err != nil {
			return added, err
		}
	}
	filePath := path.Join(config.Config.Gigapi.Root, metadata.SCHEMA_NAME, added.Table.Name, p.To)
	info, err := os.Stat(filePath)
	if err != nil {
		return added, err
	}
	added.SizeBytes = info.Size()

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return added, err
	}
	defer file.Close()
	_, err = file.Seek(-8, 2) // Seek to 8 bytes before the end of the file
	if err != nil {
		return added, err
	}

	footerMetadata := make([]byte, 8)
	_, err = file.Read(footerMetadata)
	if err != nil {
		return added, err
	}

	// The last 4 bytes contain the footer length
	added.FooterSizeBytes = int64(binary.LittleEndian.Uint32(footerMetadata[:4]))

	db, cancel, err := utils.ConnectDuckDB("")
	if err != nil {
		return added, err
	}
	defer cancel()

	added.ColumnStats = make([]metadata.ColumnDesc, len(p.From[0].ColumnStats))
	copy(added.ColumnStats, p.From[0].ColumnStats)

	populationFns := []func(string, *sql.DB, *metadata.FileDesc) error{
		d.populateMinMax,
		d.populateColStats,
		d.populateFileMetadata,
	}

	for _, f := range populationFns {
		err = f(filePath, db, &added)
		if err != nil {
			return added, err
		}
	}
	return added, nil
}

func (d *DucklakeMergeService) populateMinMax(path string, db *sql.DB, m *metadata.FileDesc) error {
	colNames, err := d.getCols(path, db)
	if err != nil {
		return err
	}
	var sel []string
	for _, col := range colNames {
		sel = append(sel, fmt.Sprintf("min(%s)::VARCHAR as %s_min", col, col))
		sel = append(sel, fmt.Sprintf("max(%s)::VARCHAR as %s_max", col, col))
	}
	minMaxQuery := fmt.Sprintf("SELECT %s FROM read_parquet($1)", strings.Join(sel, ","))
	minMaxRows, err := db.Query(minMaxQuery, path)
	if err != nil {
		return err
	}
	defer minMaxRows.Close()
	mins := make(map[string]string)
	maxs := make(map[string]string)
	for minMaxRows.Next() {
		minMaxs := make([]any, len(colNames)*2)
		for i := range minMaxs {
			s := ""
			minMaxs[i] = &s
		}
		err = minMaxRows.Scan(minMaxs...)
		if err != nil {
			return err
		}
		for i := 0; i < len(colNames); i++ {
			mins[colNames[i]] = *(minMaxs[i*2].(*string))
			maxs[colNames[i]] = *(minMaxs[i*2+1].(*string))
		}
	}

	for i := range m.ColumnStats {
		m.ColumnStats[i].Min = mins[m.ColumnStats[i].Name]
		m.ColumnStats[i].Max = maxs[m.ColumnStats[i].Name]
	}
	return nil
}

func (d *DucklakeMergeService) getCols(path string, db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT name from parquet_schema($1) where type is not null", path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var colNames []string
	for rows.Next() {
		var colName string
		err = rows.Scan(&colName)
		if err != nil {
			return nil, err
		}
		colNames = append(colNames, colName)
	}
	return colNames, nil
}

func (d *DucklakeMergeService) populateColStats(path string, db *sql.DB, m *metadata.FileDesc) error {
	rows, err := db.Query(`select 
    path_in_schema, 
    sum(total_compressed_size), 
    sum(num_values),
    sum(stats_null_count)
from parquet_metadata($1)
group by path_in_schema;`, path)
	if err != nil {
		return err
	}
	defer rows.Close()
	colStatsMap := make(map[string]metadata.ColumnDesc)
	for rows.Next() {
		var pathInSchema string
		var totalCompressedSize int64
		var numValues int64
		var nullCount int64
		err = rows.Scan(&pathInSchema, &totalCompressedSize, &numValues, &nullCount)
		if err != nil {
			return err
		}
		colStatsMap[pathInSchema] = metadata.ColumnDesc{
			Count:        numValues,
			NullCount:    nullCount,
			ContainsNans: false,
			SizeBytes:    totalCompressedSize,
		}
	}
	for i := range m.ColumnStats {
		colName := m.ColumnStats[i].Name
		if stats, ok := colStatsMap[colName]; ok {
			m.ColumnStats[i].Count = stats.Count
			m.ColumnStats[i].NullCount = stats.NullCount
			m.ColumnStats[i].ContainsNans = false
			m.ColumnStats[i].SizeBytes = stats.SizeBytes
			continue
		}
		m.ColumnStats[i].Count = 0
		m.ColumnStats[i].NullCount = 0
		m.ColumnStats[i].ContainsNans = false
		m.ColumnStats[i].SizeBytes = 0
	}
	return nil
}

func (d *DucklakeMergeService) populateFileMetadata(path string, db *sql.DB, m *metadata.FileDesc) error {
	row := db.QueryRow(`SELECT num_rows FROM parquet_file_metadata($1)`, path)
	if row.Err() != nil {
		return row.Err()
	}
	return row.Scan(&m.RecordCount)
}

func (d *DucklakeMergeService) copyColumnIDs(src string, dest string) error {
	// Read the source file's metadata
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	srcMeta, err := parquet.ReadFileMetaData(srcFile, false)
	if err != nil {
		return fmt.Errorf("failed to read source metadata: %w", err)
	}

	// Read the destination file's metadata
	destFile, err := os.OpenFile(dest, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open destination file: %w", err)
	}
	defer destFile.Close()

	destMeta, err := parquet.ReadFileMetaData(destFile, false)
	if err != nil {
		return fmt.Errorf("failed to read destination metadata: %w", err)
	}

	// Copy the schema from source to destination
	destMeta.Schema = srcMeta.Schema

	// Update column indexes in row groups

	// Write the updated metadata back to the destination file
	// Serialize the updated metadata using Thrift
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolFactory().GetProtocol(transport)
	if err := destMeta.Write(context.Background(), protocol); err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}
	serializedMeta := transport.Bytes()

	// Write the updated metadata back to the destination file
	fileSize, err := destFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of file: %w", err)
	}

	footerLength := int64(len(serializedMeta))
	_, err = destFile.Seek(fileSize-int64(footerLength)-8, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to footer start: %w", err)
	}

	if _, err := destFile.Write(serializedMeta); err != nil {
		return fmt.Errorf("failed to write updated metadata: %w", err)
	}

	// Write footer length
	if err := binary.Write(destFile, binary.LittleEndian, uint32(footerLength)); err != nil {
		return fmt.Errorf("failed to write footer length: %w", err)
	}

	// Write Parquet magic bytes
	if _, err := destFile.Write([]byte("PAR1")); err != nil {
		return fmt.Errorf("failed to write Parquet magic bytes: %w", err)
	}

	return nil
}
