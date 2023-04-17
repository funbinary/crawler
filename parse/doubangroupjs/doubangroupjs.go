package doubangroupjs

import (
	"github.com/funbinary/crawler/collect"
)

var DoubangroupJsTask = &collect.TaskModle{
	Property: collect.Property{
		Name:     "js_find_douban_sun_room",
		Cookie:   "ll=\"118201\"; __utmc=30149280; push_noty_num=0; push_doumail_num=0; __utmv=30149280.21545; __yadk_uid=CY4XlZtUkKWowjb53K8SISQTgqj8YOOU; douban-fav-remind=1; frodotk_db=\"8df2541269e216dca9d6fc373da64494\"; bid=dPuzdR0mG9M; gr_user_id=690ec6c6-4e7f-4277-b959-b829fd4aef5a; viewed=\"1007305_1475839_25913349\"; __gads=ID=613f831a31c6ac24-225718cbcadc0032:T=1679924466:RT=1679924466:S=ALNI_MaDEdHHhIEtazV6BqOobp1mDpI4Ug; __gpi=UID=00000be220b6ea7c:T=1679924466:RT=1680706111:S=ALNI_Mbp-472jjdHsL0xjpHPnuuWAacAEg; dbcl2=\"215458638:DJLz6+ZUdJ4\"; ck=V9Ki; _pk_ref.100001.8cb4=[\"\",\"\",1681392353,\"https://accounts.douban.com/\"]; _pk_id.100001.8cb4=bb24eb830bd259ee.1677888506.9.1681392353.1680706300.; _pk_ses.100001.8cb4=*; __utma=30149280.1773533084.1677888507.1680704158.1681392354.5; __utmz=30149280.1681392354.5.3.utmcsr=accounts.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmt=1; __utmb=30149280.7.5.1681392354",
		WaitTime: 2,
		MaxDepth: 5,
	},
	Root: `
		var arr = new Array();
		for (var i = 25; i <= 25; i+=25) {
			var obj = {
				Url: "https://www.douban.com/group/szsh/discussion?start=" + i,
				Priority: 1,
			    RuleName: "解析网站URL",
			    Method: "GET",
			};
            arr.push(obj);
		};
		console.log(arr[0].Url);
		AddJsReq(arr);
	`,
	Rules: []collect.RuleModle{
		{
			Name: "解析网站URL",
			ParseFunc: `
			ctx.ParseJSReg("解析阳台房","(https://www.douban.com/group/topic/[0-9a-z]+/)\"[^>]*>([^<]+)</a>");
			`,
		},
		{
			Name: "解析阳台房",
			ParseFunc: `
			//console.log("parse output");
			ctx.OutputJS("<div class=\"topic-content\">[\\s\\S]*?阳台[\\s\\S]*?<div class=\"aside\">");
			`,
		},
	},
}
