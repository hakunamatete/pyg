package com.itheima.report.util;

import java.sql.*;
import java.util.*;

public class DBOracleUtil {

    /**
     * 查询返回List<Map<String,Object>>
     * */
    /**
     * 查询
     * @param sql
     * @return
     */
    public static List<Map<String,Object>> queryList(String sql) {
        Connection conn = null;
        Statement stmt= null;
        ResultSet set = null;//查询数据库
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        try {
            conn = DBOracleConnection.getInstance();
            stmt = conn.createStatement();
            set = stmt.executeQuery(sql);
            ResultSetMetaData md = set.getMetaData();//获取表结构
            int columnCount = md.getColumnCount();//获取查询的总字段数
            while (set.next()) {   //遍历结果集
                Map<String,Object> map = new HashMap<String,Object>();
                for (int i = 1; i <= columnCount; i++) {
                    //获取i列字段名  当key  , 获取当前行的 i 列数据
                    map.put(md.getColumnName(i), set.getObject(i));
                }
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally{
            try {
                set.close();
                stmt.close();
                conn.close();
            } catch (SQLException e) {e.printStackTrace();}
        }
        return list;
    }

    /**
     * 查询返回boolean
     * */
    public static boolean queryBoolean(String sql) {
        Connection conn = null;
        Statement stmt= null;
        try {
            conn = DBOracleConnection.getInstance();
            stmt = conn.createStatement();
            ResultSet set = stmt.executeQuery(sql);
            while (set.next()) {
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally{
            try {
                stmt.close();
                conn.close();
            } catch (SQLException e) {e.printStackTrace();}
        }
        return false;
    }

    /**
     * List新增数据
     * */
    public static void executeBatch(List<String> list) {
        Connection conn = null;
        Statement stmt= null;
        try {
            conn = DBOracleConnection.getInstance();
            stmt = conn.createStatement();
            for(String sql : list){
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            try {
                stmt.close();
                conn.close();
            } catch (SQLException e) {e.printStackTrace();}
        }
    }

    /**
     * 新增数据
     * */
    public static void execute(String sql) {
        Connection conn = null;
        Statement stmt= null;
        try {
            conn = DBOracleConnection.getInstance();
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            try {
                stmt.close();
                conn.close();
            } catch (SQLException e) {e.printStackTrace();}
        }
    }


    /**
     * 	查询的话返回true，如果是更新或插入的话就返回false
     * */
    public static boolean executeBoolean(String sql) {
        Connection conn = null;
        Statement stmt= null;
        try {
            conn = DBOracleConnection.getInstance();
            stmt = conn.createStatement();
            boolean flag = stmt.execute(sql);
            stmt.close();
            conn.close();
            return flag;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取SEQ自增
     * @param seq
     * @return
     */
    public static long getSequence(String  seq){
        Connection conn = null;
        Statement stmt= null;
        long s = 0;
        try {
            conn = DBOracleConnection.getInstance();
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select "+seq+".nextval from dual");
            rs.next();
            s = rs.getLong(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return s;
    }

    public static void main(String[] args) {
        long l = getSequence("hlh_acer_head_seq");
        System.out.println(l);
    }
}
