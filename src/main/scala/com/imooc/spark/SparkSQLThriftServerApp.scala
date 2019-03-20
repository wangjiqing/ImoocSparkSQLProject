package com.imooc.spark

import java.sql.DriverManager

/**
  * 通过JDBC方式访问(Scala)
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://192.168.177.136:10000", "wangjiqing", "")

    val pstmt = conn.prepareStatement("select empno, ename, sal from emp")

    val rs = pstmt.executeQuery()
    while (rs.next()) {
      val empno = rs.getInt("empno")
      val ename = rs.getString("ename")
      val sal = rs.getDouble("sal")
      println(s"empno: $empno, ename: $ename, sal: $sal")
    }

    rs.close()
    pstmt.close()
    conn.close()
  }
}
