# encoding: utf-8
# Author: Timeashore
# Time: 2018-4-3
# Email: 1274866364@qq.com
'''
    编写目的   ：使用单线程、多线程、多进程测试爬虫速度与CPU使用率
    方法       ：单线程
    测试网站   ：链家网 https://bj.lianjia.com

    结论       ： 爬虫属于IO 密集型任务，适用多线程
    总结博文   ： http://www.cnblogs.com/ldy-miss/p/8706318.html
    知识点     ： requests,lxml
'''
import requests
from lxml import etree
import time
import pymysql

db = pymysql.connect("127.0.0.1", 'root', '123456', "test",charset="utf8")
cursor = db.cursor()
ans = 0
def save_mysql(d):
    '''保存Mysql数据库'''
    global ans
    try:
        # 执行sql语句
        print(ans,d['house_title'],d['house_location'],d['house_type'],d['house_size'],d['house_direction'],
                  d['house_salary'],d['have_seen_people'],d['pic_url'],d['house_url'])
        ans += 1
        # cursor.execute('insert into lianjia2 values(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (d['house_title'],
        #                                                                                d['house_location'],
        #                                                                                d['house_type'],
        #                                                                                d['house_size'],
        #                                                                                d['house_direction'],
        #                                                                                d['house_salary'],
        #                                                                                d['have_seen_people'],
        #                                                                                d['pic_url'],
        #                                                                                d['house_url']
        #                                                                                ))
        # db.commit()
        #提交到数据库
    except Exception as e:
        print(e)


def scrapy_begin(url):
    header = {
    "Host":"bj.lianjia.com",
    "Referer":"https://bj.lianjia.com/zufang/pg30/",
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    "Cookie":'select_city=110000; all-lj=262e01df4afab0d72dbbb5bfd7d81a6b; lianjia_uuid=bac0a1e4-1fd3-4615-9309-3551fb3b60e1; UM_distinctid=162849c39bf308-0daf08292ec244-7b113d-15f900-162849c39c056b; _jzqy=1.1522643254.1522643254.1.jzqsr=baidu|jzqct=%E9%93%BE%E5%AE%B6%E7%BD%91.-; _jzqckmp=1; gr_user_id=3c22fb92-1fac-4b79-a3fd-bc63d3fa7f78; _jzqx=1.1522649029.1522649029.1.jzqsr=bj%2Elianjia%2Ecom|jzqct=/zufang/pg30/.-; _smt_uid=5ac1b135.2334d7bf; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1522643254; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1522651854; CNZZDATA1253477573=1417529071-1522640430-https%253A%252F%252Fwww.baidu.com%252F%7C1522647881; _qzja=1.261907943.1522643253690.1522643253690.1522649029466.1522650779297.1522651853658.0.0.0.17.2; _qzjc=1; _qzjto=17.2.0; _jzqa=1.4404866957921061400.1522643254.1522643254.1522649029.2; _jzqc=1; CNZZDATA1254525948=1971825892-1522642056-https%253A%252F%252Fwww.baidu.com%252F%7C1522647906; CNZZDATA1255633284=1546167852-1522637919-https%253A%252F%252Fwww.baidu.com%252F%7C1522647785; CNZZDATA1255604082=2114176776-1522641526-https%253A%252F%252Fwww.baidu.com%252F%7C1522646820; _ga=GA1.2.443404106.1522643256; _gid=GA1.2.15591230.1522643256'
    }
    response = requests.get(url,headers=header).text
    response = etree.HTML(response)
    ul  = response.xpath('//*[@id="house-lst"]//li')
    d = dict()
    for li in ul:
        d['house_url'] = li.xpath('div[2]/h2/a/@href')[0]
        d['house_title'] = li.xpath('div[2]/h2/a/text()')[0]
        d['house_location'] = li.xpath('div[2]/div[1]/div[1]/a/span/text()')[0]
        d['house_type'] = li.xpath('div[2]/div[1]/div[1]/span[1]/span/text()')[0]
        d['house_size'] = li.xpath('div[2]/div[1]/div[1]/span[2]/text()')[0]
        d['house_direction'] = li.xpath('div[2]/div[1]/div[1]/span[3]/text()')[0]
        d['house_salary'] = li.xpath('div[2]/div[2]/div[1]/span/text()')[0]
        d['have_seen_people'] = int(li.xpath('div[2]/div[3]/div/div[1]/span/text()')[0])
        try:
            d['pic_url'] = li.xpath('div[1]/a/img/@data-img')[0]
        except:
            d['pic_url'] = 'null'
        save_mysql(d)

if __name__ == '__main__':
    q = []
    t1 = time.time()
    for x in range(1,101):
        q.append('https://bj.lianjia.com/zufang/pg{}'.format(x))

    # 开启50个线程并发执行，用时6s,比多进程快，CPU利用率高（100%）
    for url in q:
        scrapy_begin(url)
    print(time.time()-t1)








