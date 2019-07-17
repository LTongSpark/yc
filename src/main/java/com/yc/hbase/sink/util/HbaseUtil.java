package com.yc.hbase.sink.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HbaseUtil {
    /**
     * 单次查询
     *
     * @param htable    表名
     * @param rowkey    行建
     * @param family    列族
     * @param qualifier 列名
     * @return value
     */

    public static String getValue(String htable, String rowkey, String family, String qualifier) {
        Table table = null;
        String resultStr = null;
        try {
            table = HBaseDriverManager.getConnection().
                    getTable(TableName.valueOf(htable.getBytes()));
            Get get = new Get(Bytes.toBytes(rowkey));
            if (!get.isCheckExistenceOnly()) {
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                Result res = table.get(get);
                byte[] result = res.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                resultStr = Bytes.toString(result);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultStr;
    }

    /**
     * 多个rowkey查询
     *
     * @param htable    表名
     * @param rowkey    行建
     * @param family    列族
     * @param qualifier 列名
     * @return List<String>
     */

    public static List<String> getValues(String htable, List<String> rowkey, String family, String qualifier) {
        Table table = null;
        List<String> resultStr = new ArrayList<String>();
        try {
            table = HBaseDriverManager.getConnection().
                    getTable(TableName.valueOf(htable.getBytes()));
            for (String row : rowkey) {
                Get get = new Get(Bytes.toBytes(row));
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                Result res = table.get(get);
                byte[] result = res.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                resultStr.add(Bytes.toString(result));

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultStr;
    }


    /**
     * 根据指定表获取指定行键rowKey和列族family的数据 并以Map集合的形式返回查询到的结果
     *
     * @param htable 要获取表 tableName 的表名
     * @param rowKey 指定的行键rowKey
     * @param family 指定列族family
     */

    public static Map<String, String> getAllValue(String htable,
                                                  String rowKey, String family) {
        Table table = null;
        Map<String, String> resultMap = null;
        try {
            table = HBaseDriverManager.getConnection().
                    getTable(TableName.valueOf(htable.getBytes()));
            Get get = new Get(Bytes.toBytes(rowKey));
            if (!get.isCheckExistenceOnly()) {
                Result res = table.get(get);
                Map<byte[], byte[]> result = res.getFamilyMap(family.getBytes());
                Iterator<Map.Entry<byte[], byte[]>> it = result.entrySet().iterator();
                resultMap = new HashMap<String, String>();
                while (it.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = it.next();
                    resultMap.put(Bytes.toString(entry.getKey()),
                            Bytes.toString(entry.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultMap;
    }

    /**
     * 查询一段时间的值
     *
     * @param htable      表名称
     * @param startRowkey 起始rowkey
     * @param endRowkey   结束rowkey
     * @return List<String>
     */

    public static List<String> rangeScan(String htable, String startRowkey, String endRowkey) {
        Table table = null;
        List<String> resultStr = new ArrayList<String>();
        try {
            table = HBaseDriverManager.getConnection().
                    getTable(TableName.valueOf(htable.getBytes()));
            Scan scan = new Scan();
            //起始rowkey
            scan.setStartRow(Bytes.toBytes(startRowkey));
            //结束rowkey
            scan.setStopRow(Bytes.toBytes(endRowkey));
            Iterator<Result> it = table.getScanner(scan).iterator();
            while (it.hasNext()) {
                Result r = it.next();
                List<Cell> list = r.listCells();
                for (Cell cell : list) {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    resultStr.add(value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultStr;
    }

    /**
     * 模糊查询
     *
     * @param htable 表名称
     * @param regex  正则表达式
     * @return List<String>
     */

    public static List<String> rangeFilterScan(String htable, String regex) {
        Table table = null;
        List<String> resultStr = new ArrayList<String>();
        try {
            table = HBaseDriverManager.getConnection().
                    getTable(TableName.valueOf(htable.getBytes()));
            Scan scan = new Scan();
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex + ".*"));
            scan.setFilter(filter);
            Iterator<Result> it = table.getScanner(scan).iterator();
            while (it.hasNext()) {
                Result r = it.next();
                List<Cell> list = r.listCells();
                for (Cell cell : list) {
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    resultStr.add(value);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultStr;
    }

    public static void main(String[] args) {
//        Map<String, String> value = HbaseUtil.getAllValue("biz_log_affectabnormal", "1529028111100_BI_001", "yc");
//        System.out.println(value.get("infotype"));
////        for (Map.Entry<String, String> str : value.entrySet()) {
////            System.out.println(str.getKey() + "\t" + str.getValue());
////            System.out.println();
//        }

//        List<String> list = new ArrayList<String>() ;
//        list.add("1529028111100_BI_001") ;
//        list.add("1529028424016_BI_001") ;
//
//        List<String> value = HbaseUtil.getValues("biz_log_affectabnormal" ,list ,"yc" ,"timestamp") ;
//
//        for (String s : value) {
//            System.out.println(s);
//        }

//        List<String> value = HbaseUtil.rangeScan("ns1:yc" ,"1543318921896_3000060001" ,"1543319042078_3000060001","json" ,"data") ;
//
//        for (String s : value) {
//            System.out.println(s);
//        }
        List<String> value = HbaseUtil.rangeFilterScan("ns1:yc", "20181127");
        for (String s : value) {
            System.out.println(s);
        }

    }
}
