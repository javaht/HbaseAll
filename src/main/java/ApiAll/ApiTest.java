package ApiAll;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ApiTest {
    public static Connection connection = null;
    public static Admin admin = null;

    static {
        try {
            //1、获取配置信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.rootdir", "hdfs://192.168.20.62:9000/hbase");
            configuration.set("hbase.zookeeper.quorum", "192.168.20.62,192.168.20.63,192.168.20.64");
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
        admin.createTable(hTableDescriptor);
    }

    /*
     * 删除表
     *
     * */
    public static void dropTable(String tableName) throws IOException {
        //判断表存在
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在");
            return;
        }
        //让表下线
        admin.disableTable(TableName.valueOf(tableName));
        //删除表
        admin.deleteTable(TableName.valueOf(tableName));


    }

    //创建命名空间
    public static void createNameSpace(String ns) throws IOException {

        NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();

        //创建命名空间
        try {
            admin.createNamespace(build);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间已经存在");
        }


    }

    //新增数据
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2.创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));  //ctrl +p 查看需要的数据类型
        // 3.给put对象赋值(批量有两种  一种是一个rowkey 多个列  一种是多个rowkey多个列)
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("male"));


        table.put(put);

        //关闭表连接
        table.close();

    }

    // 获取数据
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建get对象

        Get get = new Get(Bytes.toBytes(rowKey));

        //指定获取的列族和列
        //  get.addFamily(Bytes.toBytes(cf));
        // 指定获取的列族和列
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        //指定获取数据的版本
        get.getMaxVersions();

        Result result = table.get(get);

        //4.解析result并且打印
        for (Cell cell : result.rawCells()) {
            //打印数据
            System.out.println("cf:" + Bytes.toString(CellUtil.cloneFamily(cell)) + ",cn:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ",value:" + Bytes.toString(CellUtil.cloneValue(cell))
            );

        }
        table.close();
    }
    //获取数据scan

    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));


        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1003"));

        //扫描表
        ResultScanner scanner = table.getScanner(scan);

        //解析scanner
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                //解析resulys
                //打印数据
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) + "cf:" + Bytes.toString(CellUtil.cloneFamily(cell)) + ",cn:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();

    }

    //删除数据
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        //
        Delete delete = new Delete(Bytes.toBytes(rowKey));


        //删除  rowkey+列族
        // Delete delete = new Delete(Bytes.toBytes(rowKey));
        //  delete.addFamily(Bytes.toBytes(cf));


        // delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));  //删除最近的时间戳的那一条
        // delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),1632749893619L);  //删除指定时间戳的那一条
        // delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn)); //删除所有的条

        //删除指定列族
        //delete.addFamily(Bytes.toBytes(cf)); //会删除列族下的所有


        table.delete(delete);
        table.close();
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
        deleteData("stu", "1003", "infodsada", "nadasme");


        //创建表测试
        //createTable("stu5","info1","info2");

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