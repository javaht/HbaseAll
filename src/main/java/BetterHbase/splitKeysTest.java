package BetterHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class splitKeysTest {

    public static Connection connection = null;
    public static Admin admin = null;
    static {
        try {
            //1、获取配置信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.rootdir", "hdfs://192.168.20.62:8020/hbase");
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            //2、创建连接对象
            connection = ConnectionFactory.createConnection(configuration);
            //3、创建Admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 创建表
     *
     * */
    public static void createTable(String tablename, String... cfs) throws IOException {
        if (cfs.length < 0) {
            System.out.println("请设置列族信息！");
            return;
        }
        //2 判断表是否存在
        if (isTableExist(tablename)) {
            System.out.println(tablename + "表已经存在");
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));
        //循环添加列族信息
        for (String cf : cfs) {
            //创建列祖描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //hColumnDescriptor.setMaxVersions();
            //添加具体的列祖信息
            hTableDescriptor.addFamily(hColumnDescriptor);

        }
        byte[][] splitkeys = new byte[4][];
        splitkeys[0]= Bytes.toBytes("1000");
        splitkeys[1]= Bytes.toBytes("2000");
        splitkeys[2]= Bytes.toBytes("3000");
        splitkeys[3]= Bytes.toBytes("4000");

        admin.createTable(hTableDescriptor,splitkeys);
    }

    /*
     * 判断表是否存在
     * */
    public static boolean isTableExist(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException {

        //删除数据
        //deleteData("stu","1009","info","name"); //测试完1006删除了最晚的一条数据 还有之前的一条数据  此时是不加s的 不加时间戳

        //删除数据
        //deleteData("stu","1007","info","name");//测试完是1007被删除 没有数据了  此时是加了s   不加时间戳(一般用这个)
        // deleteData("stu", "1003", "infodsada", "nadasme");


        //创建表测试
        createTable("staff4","info1");

        //删除表测试
        // dropTable("stu5");

        //创建命名空间测试
        // createNameSpace("0408");
        //新增数据

        //putData("stu","1005","info","name","我是小周");

        //获取数据
        //getData("stu","1001","info","name");
        //        scanTable("stu");


        //执行rowkey级别的删除

        //删除数据按照rowkey
        //deleteData("stu","1001","info","name");


        //关闭资源
        close();
    }

}
