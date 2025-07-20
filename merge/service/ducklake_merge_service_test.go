package service

import "testing"

func TestDucklakeMergeService(t *testing.T) {
	d := DucklakeMergeService{}
	err := d.copyColumnIDs("/home/hromozeka/QXIP/quackpipe/data_files/main/test/year=2025/month=7/day=9/hour=20/ducklake-0197f047-9732-77a3-9cf0-83e87462fb4d.parquet",
		"/home/hromozeka/QXIP/quackpipe/data_files/main/test/year=2025/month=7/day=9/hour=20/481e06cf-6216-4c9b-ac9c-3e2fa6be4055.2.1.parquet")
	if err != nil {
		t.Error(err)
	}
}
