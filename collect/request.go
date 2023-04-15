package collect

import (
	"crypto/md5"
	"encoding/hex"
	"github.com/funbinary/go_example/pkg/errors"
	"sync"
	"time"
)

// 任务实例
type Task struct {
	Url         string // 访问的防战
	Cookie      string
	WaitTime    time.Duration
	Reload      bool // 网站是否可以重复爬取
	MaxDepth    int
	Visited     map[string]bool
	VisitedLock sync.Mutex
	RootReq     *Request
	Fetcher     Fetcher
}

// 单个请求
type Request struct {
	Task      *Task
	Url       string
	Method    string
	Priority  int
	Depth     int
	ParseFunc func([]byte, *Request) ParseResult // 解析从网站获取到的网站信息

	unique string
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
