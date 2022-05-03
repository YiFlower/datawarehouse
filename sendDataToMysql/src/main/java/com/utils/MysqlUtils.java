package com.utils;

import java.sql.*;

public class MysqlUtils {
    private static final String URL = "jdbc:mysql://192.168.1.101:3306/mysql";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "password";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    // 获取连接
    static Connection conn = null;
    static Statement stmt = null;

    static {
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static Statement initStmt() {
        if (stmt == null) {
            new MysqlUtils();
        }
        return stmt;
    }

    // 测试连接的方法
    public static void testConnect() {
        initStmt();
        try {
            ResultSet resultSet = stmt.executeQuery("SELECT 1;");
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            System.out.println("测试完成！");
        }
    }

    // 执行SQL
    public static int execSql(String sql) {
        initStmt();
        try {
            return stmt.executeUpdate(sql);
        } catch (SQLException throwables) {
            System.out.println(sql);
            throwables.printStackTrace();
        }
        return -1;
    }

    public static void closeResources() {
        try {
            if (conn != null) conn.close();
            if (stmt != null) stmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
