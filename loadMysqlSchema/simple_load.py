import pymysql

import time

'''
by 光城
'''

class MyPyMysql:
    def __init__(self, host, port, username, password, db, charset='utf8'):
        self.host = host  # mysql主机地址
        self.port = port  # mysql端口
        self.username = username  # mysql远程连接用户名
        self.password = password  # mysql远程连接密码
        self.db = db  # mysql使用的数据库名
        self.charset = charset  # mysql使用的字符编码,默认为utf8
        self.pymysql_connect()  # __init__初始化之后，执行的函数

    def pymysql_connect(self):
        # pymysql连接mysql数据库
        # 需要的参数host,port,user,password,db,charset
        self.conn = pymysql.connect(host=self.host,
                                    port=self.port,
                                    user=self.username,
                                    password=self.password,
                                    db=self.db,
                                    charset=self.charset
                                    )
        # 连接mysql后执行的函数
        self.run()

    def run(self):
        # 创建游标
        self.cur = self.conn.cursor()

        # 定义sql语句,插入数据id,name,gender,email
        sql = 'load data local infile "/home/light/mysql/gsp/gps_1.txt" into table sqlbase fields terminated by "," lines terminated by "\n" (carflag, touchevent,opstatus,gpstime,gpslongitude,gpslatitude,gpsspeed,gpsorientation,gpsstatus);'

        self.cur.execute(sql)
        print("开始提交！\n")
        self.conn.commit()
        print("提交成功！\n")
        self.cur.close()  # 关闭游标
        self.conn.close()  # 关闭pymysql连接



if __name__ == '__main__':
    start_time = time.time()  # 计算程序开始时间
    st = MyPyMysql('127.0.0.1', 3306, 'root', 'xxxx', 'loaddb')  # 实例化类，传入必要参数
    print('程序耗时{:.2f}'.format(time.time() - start_time))  # 计算程序总耗时
