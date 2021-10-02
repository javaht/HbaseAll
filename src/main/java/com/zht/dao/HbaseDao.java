package com.zht.dao;


import com.zht.constants.Constants;
import com.zht.utils.HbaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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

        //第二部分：操作



    }

}
