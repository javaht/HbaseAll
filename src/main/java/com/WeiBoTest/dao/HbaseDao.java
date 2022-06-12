package com.WeiBoTest.dao;


import com.WeiBoTest.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/*
* 1.发布微博
* 2.删除微博
* 3.关注用户
* 4.取关用户
* 5.获取用户微博详情
* 6.获取用户的初始化页面
* */
public class HbaseDao {

    //发布微博
    public static void publishWeiBo(String uid,String content) throws IOException {
        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //1.操作微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));


        //获取当前时间戳
        long ts = System.currentTimeMillis();

        //获取rowkey
         String rowkey = uid +"_"+ ts;

        //2.put数据  创建put对象
        Put contPut = new Put(Bytes.toBytes(rowkey));

         //给put对象赋值
         contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF),Bytes.toBytes("content"),Bytes.toBytes(content));

        //执行插入数据操作
        contTable.put(contPut);


        //第二部分：操作微博收件箱表===========================================

        //获取关系表对象
        Table relatable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));


        //获取当前博主的粉丝列族数据
          Get get  =   new Get(Bytes.toBytes(uid));
          get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
          Result result = relatable.get(get);

        //创建一个集合  用于存放微博内容表的Put对象
        ArrayList<Put>  inboxPuts = new ArrayList<>();

        //遍历粉丝
        for (Cell cell : result.rawCells()) {
            //构建微博收件箱表的Put对象
            Put   inboxput  =new Put(CellUtil.cloneQualifier(cell));
            //给收件箱表的put对象赋值
            inboxput.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(uid),Bytes.toBytes(rowkey));
            //收件箱表存入集合
            inboxPuts.add(inboxput);

        }
        //判断是否有粉丝
        if (inboxPuts.size()>0) {
            //获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            //执行收件箱表插入数据操作
            inboxTable.put(inboxPuts);

            //关闭资源
            inboxTable.close();

        }
        relatable.close();
        contTable.close();
        connection.close();
    }
        //2.关注用户
    public static void addAttends(String uid,String... attends) throws IOException {

        //检验是否添加了待关注的人
        if (attends.length<=0) {
            System.out.println("请选择待关注的人！！！");
            return;
        }        /*
         * 操作用户关系表的步骤
         * 1.获取用户关系表的对象
         * 2.创建一个集合，用于存放用户关系表的PUT对象
         * 3.创建操作者的put对象
         * 4.循环创建被关注者的put对象
         * */
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //2.创建一个集合，用于存放用户关系表的PUT对象
        ArrayList<Put> relaPuts = new ArrayList<>();

        //3.创建操作者的put对象
        Put uidPut = new Put(Bytes.toBytes(uid));
        //4.循环创建被关注者的put对象
        for (String attend : attends) {
            //5.给操作者的put对象赋值
         uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1),Bytes.toBytes(attend),Bytes.toBytes(attend));
            //6.创建被关注者的put对象
            Put attendPut = new Put(Bytes.toBytes(attend));


            //7.给被关注者的put对象赋值
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2),Bytes.toBytes(uid),Bytes.toBytes(uid));


            //8.将被关注者的put对象放入集合
            relaPuts.add(attendPut);

            //将操作者的put对象添加至集合

            relaPuts.add(uidPut);

        }

        //10 执行用户关系表的插入数据操作
        relationTable.put(relaPuts);

        //第二部分代码============================

        //1.获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //2.创建收件箱表put对象
          Put inboxPut =  new Put(Bytes.toBytes(uid));

        //3.循环attends获取每个被关注者的近期发布的微博
        for (String attend : attends) {
            //4.获取当前被关注者的近期发布的微博(scan) ->集合ResultScanner

            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));

            ResultScanner resultScanner = contTable.getScanner(scan);

            //定义一个时间戳
            long ts = System.currentTimeMillis();

            //5.对获取的值进行遍历
            for (Result result : resultScanner) {
                //6.给收件箱表的Put对象赋值
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(attend),ts++,result.getRow());
            }

        }
         //7.判断当前的put对象是否为空
        if (!inboxPut.isEmpty()) {

            //获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

            //插入数据
            inboxTable.put(inboxPut);

            //关闭收件箱表连接
            inboxTable.close();
        }


    }

    //取关用户
    public  static void  deleteAttends(String uid,String... dels) throws IOException {
        if(dels.length<=0){
            System.out.println("请添加待取关的用户");
            return;
        }
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

         //第一部分：操作用户关系表
        //1.获取用户关系表对象
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
         //2.创建一个集合 用于存放用户关系表的delete对象
        ArrayList<Delete> relaDeletes = new ArrayList<>();

        //3.创建操作者的delete对象

        Delete uidDelete = new Delete(Bytes.toBytes(uid));


        //4.循环创建被取关者的delete对象

        for (String del : dels) {

            //5.给操作者的delete对象赋值

            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1),Bytes.toBytes(del));

            //6.创建被取关者的delete对象

            Delete delDelete = new Delete(Bytes.toBytes(del));

            //7.给被取关者的delete对象赋值

            delDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2),Bytes.toBytes(uid));

            //8.将被取关者的delete对象添加至集合
            relaDeletes.add(delDelete);

        }

        //将操作者的delete对象添加至集合
           relaDeletes.add(uidDelete);

        //将执行用户关系表

        relaTable.delete(relaDeletes);

        //第二部分：操作收件箱表
        //1.获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        //2.创建操作者的delete对象
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        //3.给操作者的delete对象赋值
        for (String del : dels) {

            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(del));
        }
        //执行收件箱表的删除操作
           inboxTable.delete(inboxDelete);

        //关闭
        relaTable.close();
        inboxTable.close();
        connection.close();
    }


    //获取某个人初始化页面数据
    public  static void getInit(String uid) throws IOException {
             //1.获取connction对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获取收件箱表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        //获取微博内容表对象
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //创建收件箱表Get对象 并获取数据(设置最大版本)

        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        //遍历获取的数据
        for (Cell cell : result.rawCells()) {
            //构建微博内容表Get对象

            Get contGet = new Get(CellUtil.cloneValue(cell));


            //获取get对象的数据内容
            Result contResult = contTable.get(contGet);

            //解析内容并打印
            for (Cell rawCell : contResult.rawCells()) {

                System.out.println("RK"+Bytes.toString(CellUtil.cloneRow(rawCell))+","
                        +"CF"+Bytes.toString(CellUtil.cloneFamily(rawCell))
                        + "CN"+Bytes.toString(CellUtil.cloneQualifier(rawCell))
                        + "Value"+Bytes.toString(CellUtil.cloneValue(rawCell)));
            }


        }
        //关闭资源


    }

    //获取某个人的所有微博详情

    public  static void getWeiBo(String uid) throws IOException {

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //构建scan对象
        Scan scan = new Scan();



        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(uid+"_"));
        scan.setFilter(rowFilter);



        //获取数据
        ResultScanner resultScanner = contTable.getScanner(scan);


        //解析数据并且打印
        for (Result result : resultScanner) {

            for (Cell cell : result.rawCells()) {

                System.out.println("RK"+Bytes.toString(CellUtil.cloneRow(cell))+","
                        +"CF"+Bytes.toString(CellUtil.cloneFamily(cell))
                        + "CN"+Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "Value"+Bytes.toString(CellUtil.cloneValue(cell)));
            }


        }
        contTable.close();
        connection.close();

        //关闭资源

    }


}
