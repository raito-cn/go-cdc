package main

import (
	"go-cdc/internal/cannal"
	"go-cdc/internal/db"
	_ "go-cdc/internal/log"
	_ "go-cdc/internal/model"
	"go-cdc/internal/syncdb"
	"go-cdc/pkg/config"
)

func main() {
	_, err := config.LoadConfig("config.toml")
	if err != nil {
		panic(err)
	}
	_ = db.InitCDCDataSource()
	_ = syncdb.InitOrGetDataSource()

	_ = cannal.NewFullAmountService(syncdb.DataSourceMap).Run()
}
