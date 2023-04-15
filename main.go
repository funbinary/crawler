package main

import (
	"github.com/funbinary/crawler/collect"
	"github.com/funbinary/crawler/engine"
	"github.com/funbinary/crawler/log"
	"github.com/funbinary/crawler/proxy"
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

	// douban cookie
	var seeds = make([]*collect.Task, 0, 1000)
	seeds = append(seeds, &collect.Task{
		Name:    "find_douban_sun_room",
		Fetcher: f,
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
