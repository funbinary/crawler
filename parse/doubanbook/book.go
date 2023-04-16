package doubanbook

import (
	"github.com/funbinary/crawler/collect"
	"regexp"
	"strconv"
	"time"
)

var DoubanBookTask = &collect.Task{
	Property: collect.Property{
		Name:     "douban_book_list",
		Cookie:   "ll=\"118201\"; __utmc=30149280; push_noty_num=0; push_doumail_num=0; __utmv=30149280.21545; __yadk_uid=CY4XlZtUkKWowjb53K8SISQTgqj8YOOU; douban-fav-remind=1; frodotk_db=\"8df2541269e216dca9d6fc373da64494\"; bid=dPuzdR0mG9M; gr_user_id=690ec6c6-4e7f-4277-b959-b829fd4aef5a; viewed=\"1007305_1475839_25913349\"; __gads=ID=613f831a31c6ac24-225718cbcadc0032:T=1679924466:RT=1679924466:S=ALNI_MaDEdHHhIEtazV6BqOobp1mDpI4Ug; __gpi=UID=00000be220b6ea7c:T=1679924466:RT=1680706111:S=ALNI_Mbp-472jjdHsL0xjpHPnuuWAacAEg; dbcl2=\"215458638:DJLz6+ZUdJ4\"; ck=V9Ki; _pk_ref.100001.8cb4=[\"\",\"\",1681392353,\"https://accounts.douban.com/\"]; _pk_id.100001.8cb4=bb24eb830bd259ee.1677888506.9.1681392353.1680706300.; _pk_ses.100001.8cb4=*; __utma=30149280.1773533084.1677888507.1680704158.1681392354.5; __utmz=30149280.1681392354.5.3.utmcsr=accounts.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmt=1; __utmb=30149280.7.5.1681392354",
		WaitTime: 1 * time.Second,
		MaxDepth: 5,
	},
	Rule: collect.RuleTree{Root: func() ([]*collect.Request, error) {
		roots := []*collect.Request{
			&collect.Request{
				Priority: 1,
				Url:      "https://book.douban.com",
				Method:   "GET",
				RuleName: "数据tag",
			},
		}
		return roots, nil
	},
		Trunk: map[string]*collect.Rule{
			"数据tag": &collect.Rule{ParseFunc: ParseTag},
			"书籍列表":  &collect.Rule{ParseFunc: ParseBookList},
			"书籍简介": &collect.Rule{
				ItemFields: []string{
					"书名",
					"作者",
					"页数",
					"出版社",
					"得分",
					"价格",
					"简介",
				},
				ParseFunc: ParseBookDetail,
			},
		},
	},
}

const regexpStr = `<a href="([^"]+)" class="tag">([^<]+)</a>`

func ParseTag(ctx *collect.Context) (collect.ParseResult, error) {
	re := regexp.MustCompile(regexpStr)

	matches := re.FindAllSubmatch(ctx.Body, -1)
	result := collect.ParseResult{}

	for _, m := range matches {
		result.Requesrts = append(
			result.Requesrts, &collect.Request{
				Method:   "GET",
				Task:     ctx.Req.Task,
				Url:      "https://book.douban.com" + string(m[1]),
				Depth:    ctx.Req.Depth + 1,
				RuleName: "书籍列表",
			})
	}
	// 在添加limit之前，临时减少抓取数量,防止被服务器封禁
	result.Requesrts = result.Requesrts[:1]
	return result, nil
}

const BooklistRe = `<a.*?href="([^"]+)" title="([^"]+)"`

func ParseBookList(ctx *collect.Context) (collect.ParseResult, error) {
	re := regexp.MustCompile(BooklistRe)
	matches := re.FindAllSubmatch(ctx.Body, -1)
	result := collect.ParseResult{}
	for _, m := range matches {
		req := &collect.Request{
			Method:   "GET",
			Task:     ctx.Req.Task,
			Url:      string(m[1]),
			Depth:    ctx.Req.Depth + 1,
			RuleName: "书籍简介",
		}
		req.TmpData = &collect.Temp{}
		req.TmpData.Set("book_name", string(m[2]))
		result.Requesrts = append(result.Requesrts, req)
	}
	// 在添加limit之前，临时减少抓取数量,防止被服务器封禁
	result.Requesrts = result.Requesrts[:1]

	return result, nil
}

var autoRe = regexp.MustCompile(`<span class="pl"> 作者</span>:[\d\D]*?<a.*?>([^<]+)</a>`)
var public = regexp.MustCompile(`<span class="pl">出版社:</span>([^<]+)<br/>`)
var pageRe = regexp.MustCompile(`<span class="pl">页数:</span> ([^<]+)<br/>`)
var priceRe = regexp.MustCompile(`<span class="pl">定价:</span>([^<]+)<br/>`)
var scoreRe = regexp.MustCompile(`<strong class="ll rating_num " property="v:average">([^<]+)</strong>`)
var intoRe = regexp.MustCompile(`<div class="intro">[\d\D]*?<p>([^<]+)</p></div>`)

func ParseBookDetail(ctx *collect.Context) (collect.ParseResult, error) {
	bookName := ctx.Req.TmpData.Get("book_name")
	page, _ := strconv.Atoi(ExtraString(ctx.Body, pageRe))

	book := map[string]interface{}{
		"书名":  bookName,
		"作者":  ExtraString(ctx.Body, autoRe),
		"页数":  page,
		"出版社": ExtraString(ctx.Body, public),
		"得分":  ExtraString(ctx.Body, scoreRe),
		"价格":  ExtraString(ctx.Body, priceRe),
		"简介":  ExtraString(ctx.Body, intoRe),
	}
	data := ctx.Output(book)

	result := collect.ParseResult{
		Items: []interface{}{data},
	}

	return result, nil
}

func ExtraString(contents []byte, re *regexp.Regexp) string {

	match := re.FindSubmatch(contents)

	if len(match) >= 2 {
		return string(match[1])
	} else {
		return ""
	}
}
