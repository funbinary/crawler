package main

import (
	"fmt"
	"github.com/funbinary/crawler/collect"
	"github.com/funbinary/crawler/engine"
	"github.com/funbinary/crawler/log"
	"github.com/funbinary/crawler/parse/doubangroup"
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
	for i := 0; i <= 0; i += 25 {
		str := fmt.Sprintf("https://www.douban.com/group/szsh/discussion?start=%d", i)
		seeds = append(seeds, &collect.Task{
			Url:      str,
			Fetcher:  f,
			WaitTime: 1 * time.Second,
			MaxDepth: 5,
			Cookie:   "ll=\"118201\"; __utmc=30149280; push_noty_num=0; push_doumail_num=0; __utmv=30149280.21545; __yadk_uid=CY4XlZtUkKWowjb53K8SISQTgqj8YOOU; douban-fav-remind=1; frodotk_db=\"8df2541269e216dca9d6fc373da64494\"; bid=dPuzdR0mG9M; gr_user_id=690ec6c6-4e7f-4277-b959-b829fd4aef5a; viewed=\"1007305_1475839_25913349\"; __gads=ID=613f831a31c6ac24-225718cbcadc0032:T=1679924466:RT=1679924466:S=ALNI_MaDEdHHhIEtazV6BqOobp1mDpI4Ug; __gpi=UID=00000be220b6ea7c:T=1679924466:RT=1680706111:S=ALNI_Mbp-472jjdHsL0xjpHPnuuWAacAEg; dbcl2=\"215458638:DJLz6+ZUdJ4\"; ck=V9Ki; _pk_ref.100001.8cb4=[\"\",\"\",1681392353,\"https://accounts.douban.com/\"]; _pk_id.100001.8cb4=bb24eb830bd259ee.1677888506.9.1681392353.1680706300.; _pk_ses.100001.8cb4=*; __utma=30149280.1773533084.1677888507.1680704158.1681392354.5; __utmz=30149280.1681392354.5.3.utmcsr=accounts.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmt=1; __utmb=30149280.7.5.1681392354",
			RootReq: &collect.Request{
				Priority:  1,
				ParseFunc: doubangroup.ParseURL,
			},
		})
	}

	s := engine.NewEngine(
		engine.WithLogger(logger),
		engine.WithFetcher(f),
		engine.WithSeeds(seeds),
		engine.WithWorkCount(runtime.NumCPU()),
		engine.WithScheduler(engine.NewSchedule()),
	)
	s.Run()

}
