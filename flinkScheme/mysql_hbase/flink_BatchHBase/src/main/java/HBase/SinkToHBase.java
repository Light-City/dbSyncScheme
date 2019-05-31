package HBase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import model.Student;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import java.util.List;

/**
 * by 光城
 */
public class SinkToHBase extends RichSinkFunction<List<Student>> {

    private Connection conn = null;
    private static TableName tableName = TableName.valueOf("test");
    private static final String info = "info";
    private BufferedMutator mutator;


    /**
     * open() 方法中建立连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        try {
            conn = ConnectionFactory.createConnection(config);

            System.out.println("getConnection连接成功！！！");
        } catch (IOException e) {
            System.out.println("HBase 建立连接失败 ");
        }


        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(5*1024 * 1024); //设置缓存的大小
        mutator = conn.getBufferedMutator(params);

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null){
            conn.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Student> value, Context context) throws Exception {
        int t = 0;
        //遍历数据集合
        for (Student student : value) {
//            System.out.println("databases:" + student.getDatabase());
//            System.out.println("data:" + student.getData());
//
            JSONObject jsonData = JSON.parseObject(student.getData());
            Put put = new Put(Bytes.toBytes(String.valueOf(jsonData.getInteger("id"))));
            t = jsonData.getInteger("id");


//            String carflag = jsonData.getString("carflag");
//            String touchevent = jsonData.getString("touchevent");
//            String opstatus = jsonData.getString("opstatus");
//            String gpslongitude = jsonData.getString("gpslongitude");
//            String gpslatitude = jsonData.getString("gpslatitude");
//            String gpsspeed = jsonData.getString("gpsspeed");
//            String gpsorientation = jsonData.getString("gpsorientation");
//            String gpsstatus = jsonData.getString("gpsstatus");
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("carflag"), Bytes.toBytes(jsonData.getString("carflag")));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("touchevent"), Bytes.toBytes(jsonData.getString("touchevent")));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("opstatus"), Bytes.toBytes(jsonData.getString("opstatus")));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("gpstime"), Bytes.toBytes(jsonData.getString("gpstime")));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("gpslongitude"), Bytes.toBytes(jsonData.getString("gpslongitude")));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("gpslatitude"), Bytes.toBytes(jsonData.getString("gpslatitude")));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("gpsspeed"), Bytes.toBytes(jsonData.getString("gpsspeed")));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("gpsorientation"), Bytes.toBytes(jsonData.getString("gpsorientation")));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("gpsstatus"), Bytes.toBytes(jsonData.getString("gpsstatus")));
            mutator.mutate(put);
        }
        mutator.flush();
        System.out.println("成功了插入了" + value.size() + "行数据");
        System.out.println("id:"+t);
    }
}