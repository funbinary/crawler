package main

import (
	"github.com/funbinary/crawler/collect"
	"github.com/funbinary/crawler/engine"
	"github.com/funbinary/crawler/limiter"
	"github.com/funbinary/crawler/log"
	"github.com/funbinary/crawler/proxy"
	"github.com/funbinary/crawler/storage"
	"github.com/funbinary/crawler/storage/sqlstorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
	"runtime"

	"time"
)

// xpath
func main() {
	plugin := log.NewStdoutPlugin(zapcore.InfoLevel)
	logger := log.NewLogger(plugin)
	logger.Info("log init end")
	// set zap global logger
	zap.ReplaceGlobals(logger)
	proxyURLs := []string{"http://127.0.0.1:10809", "http://127.0.0.1:10809"}
	p, err := proxy.RoundRobinProxySwitcher(proxyURLs...)
	if err != nil {
		logger.Error("RoundRobinProxySwitcher failed")
		return
	}

	var f collect.Fetcher = &collect.BrowserFetch{
		Timeout: 3000 * time.Millisecond,
		Logger:  logger,
		Proxy:   p,
	}

	var storage storage.Storage
	storage, err = sqlstorage.New(
		sqlstorage.WithSqlUrl("root:123456@tcp(127.0.0.1:3306)/crawler?charset=utf8"),
		sqlstorage.WithLogger(logger.Named("sqlDB")),
		sqlstorage.WithBatchCount(2),
	)
	if err != nil {
		logger.Error("create sqlstorage failed", zap.Error(err))
		return
	}
	//2秒钟1个
	secondLimit := rate.NewLimiter(limiter.Per(1, 2*time.Second), 1)
	//60秒20个
	minuteLimit := rate.NewLimiter(limiter.Per(20, 1*time.Minute), 20)
	multiLimiter := limiter.MultiLimiter(secondLimit, minuteLimit)
	// douban cookie
	var seeds = make([]*collect.Task, 0, 1000)
	seeds = append(seeds, &collect.Task{
		Property: collect.Property{
			Name: "douban_book_list",
		},
		Fetcher: f,
		Storage: storage,
		Limit:   multiLimiter,
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
