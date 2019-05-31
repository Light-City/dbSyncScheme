from pykafka import KafkaClient
import happybase
import json
import time

'''
by 光城
'''
class mysqlToHbase():

    def __init__(self):
        self.client = KafkaClient(hosts="localhost:9092")
        self.topic = self.client.topics['test']
        self.consumer = self.topic.get_balanced_consumer(consumer_group='sqbase', auto_commit_enable=True,
                                               zookeeper_connect='localhost:2181')
        self.conn = happybase.Connection("127.0.0.1", 9090)
        print("===========HBASE数据库表=============\n")
        print(self.conn.tables())
        self.conn.open()

    def mysqlTokafka(self,total,table_name):
        for m in self.consumer:
            table = json.loads(m.value.decode('utf-8'))["table"]
            row_data = json.loads(m.value.decode('utf-8'))["data"]
            if table=='loadTable':
                print(json.loads(m.value.decode('utf-8')))
                row_id = row_data["id"]
                row = str(row_id)
                del row_data["id"]
                data = {}
                for each in row_data:
                    neweach='info:'+each
                    data[neweach] = row_data[each]
                data['info:gpslongitude'] = str(data['info:gpslongitude'])
                data['info:gpslatitude'] = str(data['info:gpslatitude'])
                data['info:gpsspeed'] = str(data['info:gpsspeed'])
                data['info:gpsorientation'] = str(data['info:gpsorientation'])
                self.insertData(table_name,row,data)
                print(row)
                if row_id == total:
                    break
    def batchTokafka(self,start_time,table_name):
        table = self.conn.table(table_name)
        i = 1
        with table.batch(batch_size=1024*1024) as bat:
            for m in self.consumer:
                t = time.time()
                database = json.loads(m.value.decode('utf-8'))["database"]
                name = json.loads(m.value.decode('utf-8'))["table"]
                row_data = json.loads(m.value.decode('utf-8'))["data"]
                if database=='loaddb' and name == 'sqlbase':
                    row_id = row_data["id"]
                    row = str(row_id)
                    print(row_data)
                    del row_data["id"]
                    data = {}
                    for each in row_data:
                        neweach = 'info:' + each
                        data[neweach] = row_data[each]
                    data['info:gpslongitude'] = str(data['info:gpslongitude'])
                    data['info:gpslatitude'] = str(data['info:gpslatitude'])
                    data['info:gpsspeed'] = str(data['info:gpsspeed'])
                    data['info:gpsorientation'] = str(data['info:gpsorientation'])
                    # self.insertData(table_name, row, data)
                    print(data)
                    bat.put(row,data)
                    if i%1000==0:
                        print("===========插入了" + str(i) + "数据!============")
                        print("===========累计耗时：" + str(time.time() - start_time) + "s=============")
                        print("===========距离上次耗时"+ str(time.time() - t)  +"=========")
                    i+=1

    def createTable(self,table_name,families):
        self.conn.create_table(table_name,families)

    # htb.insertData(table_name, 'row', {'info:content': 'asdas', 'info:price': '299'})
    def insertData(self,table_name,row,data):
        table = self.conn.table(table_name)
        table.put(row=row,data=data)


    def deletTable(self,table_name,flag):
        self.conn.delete_table(table_name,flag)

    def getRow(self,table_name):
        table = self.conn.table(table_name)
        print(table.scan())
        i=0
        for key, value in table.scan():
            print(key)
            print(value)
            i+=1
        print(i)


    def closeTable(self):
        self.conn.close()

# total = 185941000
start_time = time.time()
htb = mysqlToHbase()
table_name = 'mysql_hbase'
families = {'info':{}}
# htb.createTable(table_name,families)
htb.batchTokafka(start_time,table_name)
htb.closeTable()


# htb.deletTable(table_name,True)
# htb.getRow(table_name)
