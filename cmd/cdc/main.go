package main

import (
	"go-cdc/internal/cannal"
	"go-cdc/internal/db"
	_ "go-cdc/internal/log"
	_ "go-cdc/internal/model"
	"go-cdc/internal/syncdb"
	"go-cdc/pkg/config"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	_, err := config.LoadConfig("config.toml")
	if err != nil {
		panic(err)
	}
	_ = db.InitCDCDataSource()
	holder := syncdb.InitOrGetDataSource()

	_ = cannal.NewFullAmountService(syncdb.DataSourceMap).Run()
	service, err := cannal.NewMysqlIncrementalService(holder["开发环境"], nil)
	if err != nil {
		panic(err)
	}
	service.Run()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	service.Stop()
}
