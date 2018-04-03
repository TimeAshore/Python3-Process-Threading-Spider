# encoding: utf-8
# Author: Timeashore
# Time: 2018-4-2
# Email: 1274866364@qq.com
'''
    编写目的   ：使用单线程、多线程、多进程测试爬虫速度与CPU使用率
    方法       ：多进程爬虫（进程池）
    测试网站   ：链家网 https://bj.lianjia.com
    
    结论       ： 爬虫属于IO 密集型任务，适用多线程
    总结博文   ： http://www.cnblogs.com/ldy-miss/p/8706318.html
    知识点     ： 多进程间共享数据，进程池，互斥锁
'''
from multiprocessing import Process,Queue,Lock,Pool,Manager
import requests
from lxml import etree
import json
import time
import pymysql
import os

# 连接Mysql
db = pymysql.connect("127.0.0.1", 'root', '123456', "test",charset="utf8")
cursor = db.cursor()

# ans = 0
def save_mysql(d, dd):
    '''保存Mysql数据库'''
    dd['ans'] += 1
    try:
        print(dd['ans'], d['house_title'], d['house_location'], d['house_type'], d['house_size'], d['house_direction'],
              d['house_salary'], d['have_seen_people'], d['pic_url'], d['house_url'])
        # ans += 1
        # 执行sql语句
        # cursor.execute('insert into lianjia values(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (d['house_title'],
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

def url_init(q):
    '''初始化,待爬URL添加进程共享数据中'''
    for x in range(1,101):
        q.put('https://bj.lianjia.com/zufang/pg{}'.format(x))

def get_url(q, lock):
    '''进程从共享数据取出一条URL'''
    #加锁
    lock.acquire()
    url = q.get()
    #释放锁
    lock.release()
    return url

def scrapy_begin(q, lock, dd):
    # 从共享数据中取出一条URL
    url = get_url(q, lock)
    header = {
    "Host":"bj.lianjia.com",
    "Referer":"https://bj.lianjia.com/zufang/pg30/",
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    "Cookie":'select_city=110000; all-lj=262e01df4afab0d72dbbb5bfd7d81a6b; lianjia_uuid=bac0a1e4-1fd3-4615-9309-3551fb3b60e1; UM_distinctid=162849c39bf308-0daf08292ec244-7b113d-15f900-162849c39c056b; _jzqy=1.1522643254.1522643254.1.jzqsr=baidu|jzqct=%E9%93%BE%E5%AE%B6%E7%BD%91.-; _jzqckmp=1; gr_user_id=3c22fb92-1fac-4b79-a3fd-bc63d3fa7f78; _jzqx=1.1522649029.1522649029.1.jzqsr=bj%2Elianjia%2Ecom|jzqct=/zufang/pg30/.-; _smt_uid=5ac1b135.2334d7bf; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1522643254; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1522651854; CNZZDATA1253477573=1417529071-1522640430-https%253A%252F%252Fwww.baidu.com%252F%7C1522647881; _qzja=1.261907943.1522643253690.1522643253690.1522649029466.1522650779297.1522651853658.0.0.0.17.2; _qzjc=1; _qzjto=17.2.0; _jzqa=1.4404866957921061400.1522643254.1522643254.1522649029.2; _jzqc=1; CNZZDATA1254525948=1971825892-1522642056-https%253A%252F%252Fwww.baidu.com%252F%7C1522647906; CNZZDATA1255633284=1546167852-1522637919-https%253A%252F%252Fwww.baidu.com%252F%7C1522647785; CNZZDATA1255604082=2114176776-1522641526-https%253A%252F%252Fwww.baidu.com%252F%7C1522646820; _ga=GA1.2.443404106.1522643256; _gid=GA1.2.15591230.1522643256'
    }
    response = requests.get(url,headers=header).text
    response = etree.HTML(response)
    ul  = response.xpath('//*[@id="house-lst"]//li')
    # with open('counts.txt','a') as fp:
    #     fp.writelines(str(len(ul))+'\n')d
    # 保存采集字段
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
        d['pic_url'] = li.xpath('div[1]/a/img/@data-img')[0]
        # with open('t.txt','a') as fp:
        #     fp.writelines(str(pic_url)+'\n')

        #存到Mysql
        save_mysql(d, dd)
        # print(house_url)
        # print(house_direction)
        # print(house_location)
        # print(house_salary)
        # print(house_size)
        # print(house_title)
        # print(house_type)
        # print(have_seen_people)
        # print(pic_url)

if __name__ == '__main__':
    # 注意，瞧黑板了！！使用进程池的时候，如果需要配上Queue,Lock等一起使用，必须用Manager里的，不然进程池不会生效。
    # 没使用进程池的情况下，使用multiprocessing里的Queue,Lock即可。
    t1 = time.time()
    manager = Manager()
    d = manager.dict()       # 设置进程共享数据，保存当前已爬数量
    d['ans'] = 0
    q = manager.Queue()      # 用Manager()中的队列
    lock = manager.Lock()    # 用Manager()中的锁

    # 启动一个进程做初始化操作
    pro_init = Process(target=url_init, args=(q,))
    pro_init.start()
    pro_init.join()

    # 创建进程池，默认同时允许执行的进程数量 = CPU核数
    pool = Pool()
    for x in range(100):
        pool.apply_async(scrapy_begin, args=(q,lock,d))     # 开启进程并行执行
    pool.close()   # 关闭进程池（pool），不再接受新的任务。
    pool.join()    # wait进程池中的全部进程结束

    # 关闭数据库连接
    # db.close()
    print(time.time()-t1)   # 耗时

    # 启动两个进程测试
    # pro_1 = Process(target=scrapy_begin, args=(q,lock))
    # pro_2 = Process(target=scrapy_begin, args=(q,lock))
    # pro_1.start()
    # pro_2.start()
    # pro_1.join()
    # pro_2.join()








