# 豆瓣热榜 top250爬虫

用挖掘机小王子的方法,生产者与消费者模式

第一步生产url,加入到urlQueue

第二步采集,循环的从urlQueue里拿,响应的response放入resQueue,这边会检测爬虫,,状态码返回418,所以用了迅代理的动态转发

第三步解析,循环的从resQueu里拿,解析页面,xpath之类的

main方法里面启动线程
