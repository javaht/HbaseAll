package Phoenix;


import java.sql.*;

/*
* 1.注册驱动  获取连接  编写sql  预编译 设置参数 执行sql  封装结果  关闭连接
*
* */
public class ThineDemo {
    public static void main(String[] args) throws SQLException {

         //获取连接

        String url = "jdbc:phoenix:thin:url=http://192.168.20.62:8765;serialization=PROTOBUF;authentication=SPENGO";
        Connection connection =  DriverManager.getConnection(url);

        //2.编写sql
        String  sql  ="select * form student ";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //执行sql
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()){
         String line =    resultSet.getString("id")+":"+resultSet.getString("name")+":"+resultSet.getString("addr");

            System.out.println(line);
          }


        resultSet.close();
        preparedStatement.close();
        connection.close();

    }
}
