package main

import (
	"github.com/funbinary/crawler/collect"
	"github.com/funbinary/crawler/collector"
	"github.com/funbinary/crawler/collector/sqlstorage"
	"github.com/funbinary/crawler/engine"
	"github.com/funbinary/crawler/log"
	"github.com/funbinary/crawler/proxy"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"runtime"

	"time"
)

// xpath
func main() {
	plugin := log.NewStdoutPlugin(zapcore.InfoLevel)
	logger := log.NewLogger(plugin)
	logger.Info("log init end")

	proxyURLs := []string{"http://127.0.0.1:10809", "http://127.0.0.1:10809"}
	p, err := proxy.RoundRobinProxySwitcher(proxyURLs...)
	if err != nil {
		logger.Error("RoundRobinProxySwitcher failed")
	}

	var f collect.Fetcher = &collect.BrowserFetch{
		Timeout: 3000 * time.Millisecond,
		Logger:  logger,
		Proxy:   p,
	}

	var storage collector.Storage
	storage, err = sqlstorage.New(
		sqlstorage.WithSqlUrl("root:123456@tcp(127.0.0.1:3306)/crawler?charset=utf8"),
		sqlstorage.WithLogger(logger.Named("sqlDB")),
		sqlstorage.WithBatchCount(2),
	)
	if err != nil {
		logger.Error("create sqlstorage failed", zap.Error(err))
		return
	}

	// douban cookie
	var seeds = make([]*collect.Task, 0, 1000)
	seeds = append(seeds, &collect.Task{
		Property: collect.Property{
			Name: "douban_book_list",
		},
		Fetcher: f,
		Storage: storage,
	})

	s := engine.NewEngine(
		engine.WithLogger(logger),
		engine.WithFetcher(f),
		engine.WithSeeds(seeds),
		engine.WithWorkCount(runtime.NumCPU()),
		engine.WithScheduler(engine.NewSchedule()),
	)
	s.Run()

}
