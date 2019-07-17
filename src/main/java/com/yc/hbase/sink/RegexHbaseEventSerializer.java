package com.yc.hbase.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * @author LTong
 */
public class RegexHbaseEventSerializer implements HbaseEventSerializer{

    //列族
    private byte[] colFam = "cf".getBytes() ;
    //获取文件
    private Event currentEvent ;

    @Override
    public void initialize(Event event, byte[] bytes) {
        //byte[] 字节型数组
        this.currentEvent = event ;
        this.colFam = bytes ;

    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
    //指定rowkey  ，单元格修饰名 ，值
    @Override
    public List<Row> getActions() {
        //切分currentEvent文件 ，从中拿到值
        String eventStr = new String(currentEvent.getBody()) ;
        String timestmap = "";
        Object time = "" ;
        Object interfaceNo = "";
        JSONObject str = JSON.parseObject(eventStr);
        try {
            if(str.containsKey("interfaceNo")){
                interfaceNo = str.getOrDefault("interfaceNo" ,"") ;
            }else{
                interfaceNo = str.getOrDefault("interfaceno" ,"") ;
            }
            Object value = str.getOrDefault("base", null);
             time = ((JSONObject) value).getOrDefault("timestamp", "");
            timestmap = DateTimeHH(time.toString()) ;
        } catch (NullPointerException e) {
            System.out.println("error");
        }catch (Exception e){
            System.out.println("logError");
        }
        byte[] currentRowKey = (timestmap +"_" +time.toString() + "_"+ interfaceNo.toString()).getBytes() ;

        //hbase的put操作
        List<Row> puts = new ArrayList<Row>() ;
        Put putJson = new Put(currentRowKey) ;
        putJson.addColumn(colFam ,"data".getBytes() ,eventStr.getBytes()) ;
        puts.add(putJson) ;
        return puts ;
    }

    @Override
    public List<Increment> getIncrements() {
        List<Increment> incs = new ArrayList<Increment>() ;
        return incs ;
    }

    @Override
    public void close() {
        colFam = null ;
        currentEvent = null ;
    }

    //将时间戳转化为单次
    public static String DateTimeHH(String timestamp) {
         return new DateTime(Long.parseLong(timestamp)).toString("yyyyMMddHHmmss") ;
    }
}
