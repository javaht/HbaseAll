package com.zht.dao;


import com.zht.constants.Constants;
import com.zht.utils.HbaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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

    }


}
