package Phoenix;

import java.sql.*;
import java.util.Properties;

public class ThickDemo {
    public static void main(String[] args) throws SQLException {

        //获取连接
        Properties props = new Properties();
        props.put("phoenix.schema.isNamespaceMappingEnabled","true");
        String url = "jdbc:phoenix:192.168.20.62,192.168.20.63,192.168.20.64:2181";
        Connection connection =  DriverManager.getConnection(url,props);

        //2.编写sql
        String  sql  ="select id,name,addr from student";

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
