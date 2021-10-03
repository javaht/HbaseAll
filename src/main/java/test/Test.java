package test;

import com.zht.constants.Constants;
import com.zht.dao.HbaseDao;
import com.zht.utils.HbaseUtil;

import java.io.IOException;

public class Test {


    public static  void init(){

        try {
            //创建命名空间
            HbaseUtil.createNameSpace(Constants.NAMESPACE);
            //创建微博内容表
            HbaseUtil.createTable(Constants.CONTENT_TABLE,Constants.CONTENT_TABLE_VERSIONS,Constants.CONTENT_TABLE_CF);
            //创建用户关系表
           HbaseUtil.createTable(Constants.RELATION_TABLE,Constants.RELATION_TABLE_VERSIONS,Constants.RELATION_TABLE_CF1,Constants.RELATION_TABLE_CF2);
            //创建收件箱表
           HbaseUtil.createTable(Constants.INBOX_TABLE,Constants.INBOX_TABLE_VERSIONS,Constants.INBOX_TABLE_CF);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //初始化
        init();

        //1001发布微博
        HbaseDao.publishWeiBo("1001","赶紧下课吧！！");

        //1002关注1001和1003
        HbaseDao.addAttends("1002","1001","1003");

        //获取1002初始化页面
        HbaseDao.getInit("1002");
        System.out.println("*****************哈哈哈哈*******************");

        //1003发布3条微博，同时1001发布2条微博

        HbaseDao.publishWeiBo("1003","这是1003第一条微博");
        Thread.sleep(10);
        HbaseDao.publishWeiBo("1001","这是1001第一条微博");
        Thread.sleep(10);
        HbaseDao.publishWeiBo("1003","这是1003第二条微博");
        Thread.sleep(10);
        HbaseDao.publishWeiBo("1001","这是1001第二条微博");
        Thread.sleep(10);
        HbaseDao.publishWeiBo("1003","这是1003第三条微博");

        //获取1002初始化页面

        HbaseDao.getInit("1002");

        //1002取关1003
        HbaseDao.deleteAttends("1001","1003");

        //获取1002初始化页面
        HbaseDao.getInit("1002");
        System.out.println("**************嘿嘿嘿嘿嘿**********************");
        //1002再次关注1003
         HbaseDao.addAttends("1002","1003");
         //获取1001微博详情
         HbaseDao.getWeiBo("1001");

    }
}
