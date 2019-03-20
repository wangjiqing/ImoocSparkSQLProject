package com.imooc.spark;

import java.sql.*;

/**
 * 通过JDBC方式访问(Java)
 */
public class SparkSQLThriftServerAppForJava {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.177.136:10000", "wangjiqing", "");
        PreparedStatement stat = conn.prepareStatement("select empno, ename, sal from emp");
        ResultSet resultSet = stat.executeQuery();
        while (resultSet.next()) {
            int empno = resultSet.getInt("empno");
            String ename = resultSet.getString("ename");
            double sal = resultSet.getDouble("sal");
            System.out.println("empno:" + empno + ",ename:" + ename + ",sal:" + sal);
        }

        resultSet.close();
        stat.close();
        conn.close();
    }
}
