package com.itheima.report.util;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * 数据库连接池
 * */
public class DBOracleConnection {
    private  DBOracleConnection(){}
    // 本类对象实例化
    private static DBOracleConnection oracle = new DBOracleConnection();
    private static Connection conn = null;

    // 返回本类对象的静态方法
    public static DBOracleConnection getSingleton() {
        if (null == oracle) {
            oracle = new DBOracleConnection();
        }
        return oracle;
    }

    public static synchronized Connection getInstance() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection( "jdbc:oracle:thin:@192.168.1.245:1521:PROD", "b2b", "b2b");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static synchronized Connection getOldInstance() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection( "jdbc:oracle:thin:@172.16.0.165:31521/PDB", "hmall", "hmall2018Hlh");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}
