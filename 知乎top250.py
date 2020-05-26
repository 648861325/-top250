from typing import Optional, Callable, Any, Iterable, Mapping

import requests
import threading
from lxml import etree
import queue
import pymysql
import time
import hashlib
import urllib3

# 关闭警告信息
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# 生产网址,生产者搞定
def getUrlQueue():
    urlqueue = queue.Queue()
    base_url = 'https://movie.douban.com/top250?start={}'

    for i in range(0,10):
        url = base_url.format(i * 25)

        # 把url放入队列
        urlqueue.put(url)
    return urlqueue


# 动态转发  没用上
def down(url):
    '''
    使用讯代理下载指定的页面
    :param url:
    :return:
    '''
    orderno = "xxxxxxxxxxxxxxxxxx"  # 订单号
    secret = "xxxxxxxxxxxxxxxxxx"  # 秘钥
    ip = "forward.xdaili.cn"
    port = "80"
    ip_port = ip + ":" + port
    nums = 1
    while nums <= 3:
        # 生成签名参数sign
        timestamp = str(int(time.time()))  # 10位的时间戳
        string = "orderno=" + orderno + "," + "secret=" + secret + "," + "timestamp=" + timestamp
        string = string.encode()
        md5_string = hashlib.md5(string).hexdigest()
        sign = md5_string.upper()
        # auth验证信息
        auth = "sign=" + sign + "&" + "orderno=" + orderno + "&" + "timestamp=" + timestamp
        # 代理ip
        proxy = {"http": "http://" + ip_port, "https": "https://" + ip_port}
        # 请求头
        headers = {"Proxy-Authorization": auth,
                   "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36"
                   }
        try:
            # 通过代理IP进行请求
            response = requests.get(url, headers=headers, proxies=proxy, verify=False, allow_redirects=False)
            if response.status_code == 200:
                return response
            else:
                nums += 1
        except Exception as e:
            nums += 1
    return None


# 采集线程组
class spiderThread(threading.Thread):
    # 初始化方法
    def __init__(self, name, urlqueue):
        # 初始化父类,name是线程名字
        super().__init__(name=name)
        # 拿到网址,初始化网址队列
        self.urlqueue = urlqueue

    # 运行线程的方法,重写run
    def run(self):
        # 在这里实现数据采集,获取response
        print(f'{self.name}采集线程正在执行')

        # 循环提取数据,队列不为空就执行
        while not self.urlqueue.empty():
            try:
                url = self.urlqueue.get(block=False)
                # headers = {
                #     'UserAgent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
                # }
                # response = requests.get(url,headers=headers)

                response = down(url)

                # 把响应放入缓冲区队列
                responsequeue.put(response.text)

            except:
                pass


# 解析线程组
class parseThread(threading.Thread):
    # 初始化方法
    def __init__(self, name, responsequeue,lock):
        # 初始化父类,name是线程名字
        super().__init__(name=name)
        # 拿到响应,初始化响应队列
        self.responsequeue = responsequeue
        self.conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='douban')
        self.cur = self.conn.cursor()
        self.lock = lock

    # 运行线程的方法,重写run
    def run(self):
        # 在这里实现数据采集,获取response
        print(f'{self.name}解析线程正在执行')
        while parse_exit_flag:
            try:
                # 获取response响应
                response = responsequeue.get(block=False)
                # response = response.text
                # 调用解析方法
                self.parseresponse(response)
            except:
                pass

    def parseresponse(self, response):

        html = etree.HTML(response)
        ls = html.xpath('//div[@class="article"]/ol/li')
        for i in ls:
            num = i.xpath('./div[@class="item"]/div[1]/em/text()')[0]
            print(num)

            title = i.xpath('./div[@class="item"]/div[2]/div[1]/a/span[1]/text()')[0]
            print(title)
            daoyan = i.xpath('./div[@class="item"]/div[2]/div[2]/p[1]/text()')[0].strip()
            print(daoyan)
            pingfen = i.xpath('./div[@class="item"]/div[2]/div[2]/div/span[2]/text()')[0]
            print(pingfen)
            pingjiashu = i.xpath('./div[@class="item"]/div[2]/div[2]/div/span[4]/text()')[0]
            print(pingjiashu)
            tag = i.xpath('./div[@class="item"]/div[2]/div[2]/p[2]/span/text()')[0]
            print(tag)

            sql = 'insert into thread values(0,%s,%s,%s,%s,%s,%s)'

            param = (num, title, daoyan, pingfen, pingjiashu, tag)

            # 互斥锁
            self.lock.acquire()
            self.cur.execute(sql, param)
            self.conn.commit()

            self.lock.release()

    def close_spider(self):
        self.cur.close()
        self.conn.close()


if __name__ == '__main__':
    lock = threading.Lock()

    start=time.time()

    print('主线程开始')
    # 生产者生产网址
    # 第一步 启动生产者
    urlqueue = getUrlQueue()
    responsequeue = queue.Queue()

    parse_exit_flag = True

    # 第二步  启动采集线程组
    spiderlist = []
    for i in range(10):
        # 传入不同的名字,网址队列全部传递过去
        t = spiderThread(f'spider{i}', urlqueue)
        spiderlist.append(t)  # 线程组

    for t in spiderlist:
        t.start()  # 启动

    # 第三步,启动解析线程
    parselist = []
    for i in range(10):
        # 传入不同的名字,网址队列全部传递过去
        t = parseThread(f'spider{i}', responsequeue,lock=lock)
        parselist.append(t)  # 线程组

    for t in parselist:
        t.start()  # 启动

    # 判断urlqueue是否为空,
    # 如果它不为空就一直阻塞在这里,不往下执行
    while not urlqueue.empty():
        pass

    # 采集线程的
    for t in spiderlist:
        t.join()

    # 判断responsequeue是否为空,
    # 如果它不为空就一直阻塞在这里,不往下执行
    while not responsequeue.empty():
        pass

    # 在这个位置以上,前面的都结束了
    parse_exit_flag = False

    # 解析线程
    for t in parselist:
        t.join()  # 启动

    print('主线程退出')
    stop=time.time()
    numtime=start-stop
    print('总用时',numtime)
