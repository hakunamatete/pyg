package com.itheima.report;

import com.itheima.report.util.DBOracleUtil;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class OrderJDBCTest {

    @Test
    public void sendTest01(){
        List<Map<String,Object>> listTable =  DBOracleUtil.queryList("select table_name from user_tables order by table_name");
        Iterator<Map<String, Object>> it = listTable.iterator();
        while (it.hasNext()) {
            Map<String, Object> map = (Map<String, Object>) it.next();
            System.out.println(map.get("TABLE_NAME"));
        }
    }

}
