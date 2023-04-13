package engine

import (
	"github.com/funbinary/crawler/collect"
	"go.uber.org/zap"
	"sync"
)

type Crawler struct {
	out         chan collect.ParseResult //负责处理爬取后的数据，完成下一步的存储操作。schedule 函数会创建调度程序，负责的是调度的核心逻辑。
	Visited     map[string]bool          //存储请求访问信息
	VisitedLock sync.Mutex
	options
}

func NewEngine(opts ...Option) *Crawler {
	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	e := &Crawler{}
	e.Visited = make(map[string]bool, 100)
	e.options = options
	return e
}

func (e *Crawler) Run() {
	go e.Schedule()
	// 创建指定数量的 worker，完成实际任务的处理
	// 其中
	for i := 0; i < e.WorkCount; i++ {
		go e.CreateWork()
	}
	e.HandleResult()
}

func (e *Crawler) Schedule() {
	// workerCh
	var reqs []*collect.Request
	for _, seed := range e.Seeds {
		seed.RootReq.Task = seed
		seed.RootReq.Url = seed.Url
		reqs = append(reqs, seed.RootReq)
	}
	go e.scheduler.Schedule()
	go e.scheduler.Push(reqs...)

}

func (e *Crawler) CreateWork() {
	for {
		// 接收到调度器分配的任务；
		r := e.scheduler.Pull()
		if err := r.Check(); err != nil {
			e.Logger.Error("check failed", zap.Error(err))
			continue
		}
		// 判断是否已经访问过
		if e.HasVisited(r) {
			e.Logger.Debug("request has visited",
				zap.String("url", r.Url),
			)
			continue
		}
		e.StoreVisited(r)

		// 访问服务器
		body, err := r.Task.Fetcher.Get(r)
		if len(body) < 6000 {
			e.Logger.Error(
				"can't fetch",
				zap.Int("length", len(body)),
				zap.String("url", r.Url),
			)
			continue
		}
		if err != nil {
			e.Logger.Error(
				"can't fetch ",
				zap.Error(err),
				zap.String("url", r.Url),
			)
			continue
		}
		//解析服务器返回的数据
		result := r.ParseFunc(body, r)
		if len(result.Requesrts) > 0 {
			go e.scheduler.Push(result.Requesrts...)
		}
		// 将返回的数据发送到 out 通道中，方便后续的处理。
		e.out <- result
	}
}

func (e *Crawler) HandleResult() {
	for {
		select {
		// 接收所有 worker 解析后的数据
		case result := <-e.out:
			//包含了我们实际希望得到的结果，所以我们先用日志把结果打印出来
			for _, item := range result.Items {
				// todo: store
				e.Logger.Sugar().Info("get result", item)
			}
		}
	}
}

func (e *Crawler) HasVisited(r *collect.Request) bool {
	e.VisitedLock.Lock()
	defer e.VisitedLock.Unlock()
	unique := r.Unique()
	return e.Visited[unique]
}

func (e *Crawler) StoreVisited(reqs ...*collect.Request) {
	e.VisitedLock.Lock()
	defer e.VisitedLock.Unlock()

	for _, r := range reqs {
		unique := r.Unique()
		e.Visited[unique] = true
	}
}

type Scheduler interface {
	Schedule()                //启动调度器
	Push(...*collect.Request) //将请求放入到调度器中
	Pull() *collect.Request   //从调度器中获取请求
}

type Schedule struct {
	requestCh   chan *collect.Request //负责接收请求
	workerCh    chan *collect.Request //负责分配任务给 worker
	priReqQueue []*collect.Request
	reqQueue    []*collect.Request
	Logger      *zap.Logger
}

func NewSchedule() *Schedule {
	s := &Schedule{}
	requestCh := make(chan *collect.Request)
	workerCh := make(chan *collect.Request)
	s.requestCh = requestCh
	s.workerCh = workerCh
	return s
}

func (s *Schedule) Schedule() {
	var req *collect.Request
	var ch chan *collect.Request
	go func() {
		for {
			if req == nil && len(s.priReqQueue) > 0 {
				req = s.priReqQueue[0]
				s.priReqQueue = s.priReqQueue[1:]
				ch = s.workerCh
			}

			//如果任务队列 reqQueue 大于 0，意味着有爬虫任务，这时我们获取队列中第一个任务，并将其剔除出队列。
			if req == nil && len(s.reqQueue) > 0 {
				req = s.reqQueue[0]
				s.reqQueue = s.reqQueue[1:]
				ch = s.workerCh
			}
			select {
			case r := <-s.requestCh:
				if r.Priority > 0 {
					s.priReqQueue = append(s.priReqQueue, r)
				} else {
					// 接收来自外界的请求，并将请求存储到 reqQueue 队列中
					s.reqQueue = append(s.reqQueue, r)
				}
			case ch <- req:
				// ch <- req 会将任务发送到 workerCh 通道中，等待 worker 接收。
				req = nil
				ch = nil
			}
		}
	}()

}

func (s *Schedule) Push(reqs ...*collect.Request) {
	for _, req := range reqs {
		s.requestCh <- req
	}
}

func (s *Schedule) Pull() *collect.Request {
	r := <-s.workerCh
	return r
}

func (s *Schedule) Output() *collect.Request {
	r := <-s.workerCh
	return r
}
