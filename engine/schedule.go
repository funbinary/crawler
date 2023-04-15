package engine

import (
	"github.com/funbinary/crawler/collect"
	"github.com/funbinary/crawler/parse/doubangroup"
	"github.com/robertkrimen/otto"
	"go.uber.org/zap"

	"sync"
)

func init() {
	Store.Add(doubangroup.DoubangroupTask)
	Store.AddJSTask(doubangroup.DoubangroupJsTask)
}

var Store = &CrawlerStore{
	list: []*collect.Task{},
	hash: map[string]*collect.Task{},
}

type CrawlerStore struct {
	list []*collect.Task
	hash map[string]*collect.Task
}

func (c *CrawlerStore) Add(task *collect.Task) {
	c.hash[task.Name] = task
	c.list = append(c.list, task)
}

type mystruct struct {
	Name string
	Age  int
}

// 用于动态规则添加请求。
func AddJsReqs(jreqs []map[string]interface{}) []*collect.Request {
	reqs := make([]*collect.Request, 0)

	for _, jreq := range jreqs {
		req := &collect.Request{}
		u, ok := jreq["Url"].(string)
		if !ok {
			return nil
		}
		req.Url = u
		req.RuleName, _ = jreq["RuleName"].(string)
		req.Method, _ = jreq["Method"].(string)
		req.Priority, _ = jreq["Priority"].(int64)
		reqs = append(reqs, req)
	}
	return reqs
}

// 用于动态规则添加请求。
func AddJsReq(jreq map[string]interface{}) []*collect.Request {
	reqs := make([]*collect.Request, 0)
	req := &collect.Request{}
	u, ok := jreq["Url"].(string)
	if !ok {
		return nil
	}
	req.Url = u
	req.RuleName, _ = jreq["RuleName"].(string)
	req.Method, _ = jreq["Method"].(string)
	req.Priority, _ = jreq["Priority"].(int64)
	reqs = append(reqs, req)
	return reqs
}

func (c *CrawlerStore) AddJSTask(m *collect.TaskModle) {
	task := &collect.Task{
		Property: m.Property,
	}

	task.Rule.Root = func() ([]*collect.Request, error) {
		vm := otto.New()
		vm.Set("AddJsReq", AddJsReqs)
		v, err := vm.Eval(m.Root)
		if err != nil {
			return nil, err
		}
		e, err := v.Export()
		if err != nil {
			return nil, err
		}
		return e.([]*collect.Request), nil
	}

	for _, r := range m.Rules {
		paesrFunc := func(parse string) func(ctx *collect.Context) (collect.ParseResult, error) {
			return func(ctx *collect.Context) (collect.ParseResult, error) {
				vm := otto.New()
				vm.Set("ctx", ctx)
				v, err := vm.Eval(parse)
				if err != nil {
					return collect.ParseResult{}, err
				}
				e, err := v.Export()
				if err != nil {
					return collect.ParseResult{}, err
				}
				if e == nil {
					return collect.ParseResult{}, err
				}
				return e.(collect.ParseResult), err
			}
		}(r.ParseFunc)
		if task.Rule.Trunk == nil {
			task.Rule.Trunk = make(map[string]*collect.Rule, 0)
		}
		task.Rule.Trunk[r.Name] = &collect.Rule{
			paesrFunc,
		}
	}

	c.hash[task.Name] = task
	c.list = append(c.list, task)
}

type Crawler struct {
	out         chan collect.ParseResult //负责处理爬取后的数据，完成下一步的存储操作。schedule 函数会创建调度程序，负责的是调度的核心逻辑。
	Visited     map[string]bool          //存储请求访问信息
	VisitedLock sync.Mutex
	failures    map[string]*collect.Request // 失败请求id -> 失败请求
	failureLock sync.Mutex
	options
}

func NewEngine(opts ...Option) *Crawler {
	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	e := &Crawler{}
	e.Visited = make(map[string]bool, 100)
	e.failures = make(map[string]*collect.Request)
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

// Schedule 从seed种子任务添加到任务列表中, 并启动调度
func (e *Crawler) Schedule() {
	// workerCh
	var reqs []*collect.Request
	for _, seed := range e.Seeds {
		task := Store.hash[seed.Name]
		task.Fetcher = seed.Fetcher
		rootreqs, err := task.Rule.Root()
		if err != nil {
			e.Logger.Error("get root failed",
				zap.Error(err),
			)
			continue
		}
		for _, req := range rootreqs {
			req.Task = task
		}
		reqs = append(reqs, rootreqs...)
	}
	go e.scheduler.Schedule()
	go e.scheduler.Push(reqs...)

}

func (e *Crawler) CreateWork() {
	// 获取任务,然后执行, 解析
	for {
		// 接收到调度器分配的任务；
		req := e.scheduler.Pull()
		if err := req.Check(); err != nil {
			e.Logger.Error("check failed", zap.Error(err))
			continue
		}
		// 判断是否已经访问过
		if !req.Task.Reload && e.HasVisited(req) {
			e.Logger.Debug("request has visited",
				zap.String("url", req.Url),
			)
			continue
		}
		e.StoreVisited(req)

		// 访问服务器
		body, err := req.Task.Fetcher.Get(req)
		if err != nil {
			e.Logger.Error(
				"can't fetch ",
				zap.Error(err),
				zap.String("url", req.Url),
			)
			e.SetFailure(req)
			continue
		}
		if len(body) < 6000 {
			e.Logger.Error(
				"can't fetch",
				zap.Int("length", len(body)),
				zap.String("url", req.Url),
			)
			e.SetFailure(req)
			continue
		}

		rule := req.Task.Rule.Trunk[req.RuleName]

		result, err := rule.ParseFunc(&collect.Context{
			Body: body,
			Req:  req,
		})

		if err != nil {
			e.Logger.Error("ParseFunc failed ",
				zap.Error(err),
				zap.String("url", req.Url),
			)
			continue
		}

		//解析服务器返回的数据
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

func (e *Crawler) SetFailure(req *collect.Request) {
	if !req.Task.Reload {
		e.VisitedLock.Lock()
		unique := req.Unique()
		delete(e.Visited, unique)
		e.VisitedLock.Unlock()
	}
	e.failureLock.Lock()
	defer e.failureLock.Unlock()
	if _, ok := e.failures[req.Unique()]; !ok {
		// 首次失败时，再重新执行一次
		e.failures[req.Unique()] = req
		e.scheduler.Push(req)
	}
	// todo: 失败2次，加载到失败队列中
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
	// 从请求管道获取任务,添加到队列
	// 从队列中获取任务, 发送到执行管道
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
