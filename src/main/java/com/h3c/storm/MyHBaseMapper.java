package com.h3c.storm;

import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.json.JSONObject;

import backtype.storm.tuple.Tuple;
import clojure.string__init;

public class MyHBaseMapper implements HBaseMapper {
        
	  static int boltcount = 0;
	  
	  public ColumnList columns(Tuple tuple) {
	    
	    ColumnList cols = new ColumnList();
	    
	    /* 解析消息 */
	    String clientMsg = tuple.getStringByField("clientInfo");
	    
	    /* covert string to json object */
	    JSONObject clientMsgObject = new JSONObject(clientMsg); 
	    
	    String acSNStr = clientMsgObject.getString("acSN");
	    String clientMacStr = clientMsgObject.getString("clientMAC");
	    String onLineTimeStr = clientMsgObject.getString("onLineTime");
	    
	    //参数依次是列族名，列名，值
	    cols.addColumn("f1".getBytes(), "clientMAC".getBytes(), clientMacStr.getBytes());
	    cols.addColumn("f1".getBytes(), "acSN".getBytes(), acSNStr.getBytes());
	    cols.addColumn("f1".getBytes(), "onLineTime".getBytes(), onLineTimeStr.getBytes());
	    
	    System.err.println("BOLT + " + "acSN="+ acSNStr + "clientMacStr = " + clientMacStr + "onLineTimeStr" + onLineTimeStr);
	    
	    boltcount++;
	    System.err.println("BOLT count = " + boltcount);
	    
	    //System.err.println("BOLT + " + tuple.getStringByField("clientInfo"));
	    //cols.addColumn("f1".getBytes(), "hhhhhhh".getBytes(), "0000-0000-0001".getBytes());
	    //System.err.println("BOLT + " + tuple.getStringByField("clientInfo"));
	    return cols;
	  }

	  public byte[] rowKey(Tuple tuple) {
	             
	    //return tuple.getStringByField("clientInfo").getBytes();
	    
	    String clientMsg = tuple.getStringByField("clientInfo");
	    
	    JSONObject clientMsgObject = new JSONObject(clientMsg); 
	    
	    String acSNStr = clientMsgObject.getString("acSN");
	    String clientMacStr = clientMsgObject.getString("clientMAC");
	    String onLineTimeStr = clientMsgObject.getString("onLineTime");
	    
	    String RowKey = acSNStr+clientMacStr+onLineTimeStr;
	    
		return RowKey.getBytes(); 
	  }
	}