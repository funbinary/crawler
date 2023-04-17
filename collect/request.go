package collect

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"github.com/funbinary/crawler/limiter"
	"github.com/funbinary/crawler/storage"
	"github.com/funbinary/go_example/pkg/errors"
	"go.uber.org/zap"
	"math/rand"
	"regexp"
	"sync"
	"time"
)

// 任务的公共属性

type Property struct {
	Name     string // 用户界面显示的名称（应保证唯一性）
	Url      string // 访问的防战
	Cookie   string
	WaitTime int64 // 随机休眠,秒
	Reload   bool  // 网站是否可以重复爬取
	MaxDepth int64
}

// 任务实例
type Task struct {
	Property
	Visited     map[string]bool
	VisitedLock sync.Mutex
	Rule        RuleTree
	Fetcher     Fetcher
	Storage     storage.Storage
	Logger      *zap.Logger
	Limit       limiter.RateLimiter
}

type Context struct {
	Body []byte
	Req  *Request
}

func (c *Context) GetRule(ruleName string) *Rule {
	return c.Req.Task.Rule.Trunk[ruleName]
}

func (c *Context) Output(data interface{}) *storage.DataCell {
	res := &storage.DataCell{}
	res.Data = make(map[string]interface{})
	res.Data["Task"] = c.Req.Task.Name
	res.Data["Rule"] = c.Req.RuleName
	res.Data["Data"] = data
	res.Data["Url"] = c.Req.Url
	res.Data["Time"] = time.Now().Format("2006-01-02 15:04:05")
	return res
}

func (c *Context) ParseJSReg(name string, reg string) ParseResult {
	re := regexp.MustCompile(reg)

	matches := re.FindAllSubmatch(c.Body, -1)
	result := ParseResult{}

	for _, m := range matches {
		u := string(m[1])
		result.Requesrts = append(
			result.Requesrts, &Request{
				Method:   "GET",
				Task:     c.Req.Task,
				Url:      u,
				Depth:    c.Req.Depth + 1,
				RuleName: name,
			})
	}
	return result
}

func (c *Context) OutputJS(reg string) ParseResult {
	re := regexp.MustCompile(reg)
	ok := re.Match(c.Body)
	if !ok {
		return ParseResult{
			Items: []interface{}{},
		}
	}
	result := ParseResult{
		Items: []interface{}{c.Req.Url},
	}
	return result
}

// 单个请求
type Request struct {
	Task     *Task
	Url      string
	Method   string
	Priority int64
	Depth    int64
	RuleName string

	unique  string
	TmpData *Temp
}

func (r *Request) Fetch() ([]byte, error) {
	if err := r.Task.Limit.Wait(context.Background()); err != nil {
		return nil, err
	}
	// 随机休眠，模拟人类行为
	sleeptime := rand.Int63n(r.Task.WaitTime * 1000)
	time.Sleep(time.Duration(sleeptime) * time.Millisecond)
	return r.Task.Fetcher.Get(r)
}

// 请求的唯一识别码
func (r *Request) Unique() string {
	block := md5.Sum([]byte(r.Url + r.Method))
	return hex.EncodeToString(block[:])
}

func (r *Request) Check() error {
	if r.Depth > r.Task.MaxDepth {
		return errors.New("Max depth limit reached")
	}
	return nil
}

type ParseResult struct {
	Requesrts []*Request
	Items     []interface{}
}
