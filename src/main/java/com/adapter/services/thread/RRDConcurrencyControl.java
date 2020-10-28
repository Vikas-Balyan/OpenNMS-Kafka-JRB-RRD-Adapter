package com.adapter.services.thread;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.jrobin.core.FetchData;
import org.jrobin.core.FetchRequest;
import org.jrobin.core.RrdDb;
import org.json.JSONArray;
import org.json.JSONObject;



public class RRDConcurrencyControl implements Callable<String> {
	private String path;
	private long startDate;
	private long endDate;
	private String  nmsIp;
	
	
	public RRDConcurrencyControl(String nmsIp, String path, long startDate, long endDate) {
		super();
		this.nmsIp=nmsIp;
		this.path = path;
		this.startDate = startDate;
		this.endDate = endDate;
	}




	@Override
	public String call() throws Exception {
		JSONObject dataObj=null;
		RrdDb rrd = new RrdDb(path, true);
        FetchRequest request = rrd.createFetchRequest("AVERAGE", startDate, endDate);
        FetchData fetchData = request.fetchData();
        String[] lines = fetchData.dump().split("\n");
        String[] header = fetchData.getDsNames();
        for(String line:Arrays.asList(lines)) {      	
        	String parts[]=line.split("  ");
        	long timestamp=Long.valueOf(parts[0].split(":")[0]);
        	JSONArray values=new JSONArray();
        	for(int ind=0;ind<header.length;ind++) {
        		values.put(parts[ind+1]);
        	}
        	dataObj=new JSONObject();
        	dataObj.put("time", timestamp);
        	dataObj.put("path", path); // get interface,node id and other details 
        	dataObj.put("values", values);
        	dataObj.put("nms", nmsIp);
        	String pathParts[]=path.split("/");
        	String mib=pathParts[pathParts.length-1];
        	dataObj.put("topic", mib.split(".jrb")[0].toLowerCase());
        	break;
        }
		rrd.close();
		return dataObj.toString();
	}
	
  }


