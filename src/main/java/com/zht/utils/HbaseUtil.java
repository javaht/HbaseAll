package com.zht.utils;


import com.zht.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;

import java.io.IOException;

/*
* 1.创建命名空间
* 2.判断表是否存在
* 3.创建表(三张表)
*
* */
public class HbaseUtil {
    //创建命名空间
    public static void createNameSpace(String nameSpace) throws IOException {
        //1.获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //2.获取admin对象
        Admin admin = connection.getAdmin();
        //3.构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //创建命名空间
        admin.createNamespace(namespaceDescriptor);
        //关闭资源
        admin.close();
       connection.close();
    }

    //判断表是否存在
    private static boolean isTableExists(String  tableName) throws IOException {
        //1.获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //2.获取admin对象
        Admin admin = connection.getAdmin();
        //3.判断是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        //4.关闭资源
        admin.close();
        connection.close();
        //5.返回结果
        return exists;
    }

    //创建表
    public  static  void createTable(String tableName,int versions,String... cfs) throws IOException {
        //判断是否传入了列族信息
        if(cfs.length<=0){
            System.out.println("请设置列族信息！！");
            return;
        }
         //判断表是否存在
          if(isTableExists(tableName)){
              System.out.println(tableName+"表已经存在");
              return;
          }
        //获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //获取admin对象
        Admin admin = connection.getAdmin();
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //循环添加列族信息
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //设置版本
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //创建表
        admin.createTable(hTableDescriptor);
        //关闭资源
        admin.close();
        connection.close();



    }





}
