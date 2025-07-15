package metadata

import (
	"context"
	"fmt"
	"testing"
)

func TestMetadata(t *testing.T) {
	files, err := GetFiles("", 1, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		fmt.Println(file)
	}
	var colStats []ColumnDesc
	var partValues []FilePartitionValue
	for i := range files {
		if files[i].FilePartitionValues != nil {
			partValues = files[i].FilePartitionValues
		}
		if files[i].ColumnStats != nil {
			colStats = files[i].ColumnStats
		}
	}

	addFile := FileDesc{
		Id:                  0,
		Table:               files[0].Table,
		Path:                "/year=2025/month=7/day=9/hour=20/ducklake-0197f047-9732-77a3-9cf0-83e87462fb4d.parquet",
		SizeBytes:           506,
		FooterSizeBytes:     272,
		RecordCount:         2,
		ColumnStats:         colStats,
		FilePartitionValues: partValues,
		PartitionId:         files[0].PartitionId,
	}
	err = FinishMerge(context.Background(),
		files,
		[]FileDesc{addFile},
		&files[0].Table,
	)
	if err != nil {
		t.Fatal(err)
	}
}
