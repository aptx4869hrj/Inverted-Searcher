package com.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.huaban.analysis.jieba.JiebaSegmenter;

public class searcher extends HttpServlet{
	
	private static final long serialVersionUID = 1L;
	
	public searcher() {
		super();
	}
	@Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req,resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	
    	String word = req.getParameter("searcher");
    	System.out.println(word);
    	searcher sc = new searcher();
    	//sc.start(word);
    }

	public static void main(String[] args) {
//	private static void start(String searcher){
		System.setProperty("hadoop.home.dir", "C:\\Users\\HRJ\\hadoop-2.7.7");

		//创建配置对象
		Configuration config = HBaseConfiguration.create();
		//配置zookeeper集群
		config.set("hbase.zookeeper.quorum", "172.17.11.141,172.17.11.144,172.17.11.145");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		//配置hmaster地址
		config.set("hbase.master", "172.17.11.231:16000");

		
		try {
			//获取hbase数据库的链接
			Connection connection = ConnectionFactory.createConnection(config);
			//根据表名获取表
			Table table = connection.getTable(TableName.valueOf("urlData"));	
			scantable(table,446);
			
			//table.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void scantable(Table table,int limit) throws IOException {
		int i=0;
		ArrayList<String> searchre = new ArrayList<String>();
		Scanner scanKey = new Scanner(System.in);
		System.out.println("输入：");
		
		String key = scanKey.next();
		
		Scan scan = new Scan();
		
		//Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(key)));
		//Filter filter = new PrefixFilter(Bytes.toBytes("01"));
		//Filter filter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator(key));
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("key"), Bytes.toBytes("key"), CompareOp.EQUAL, Bytes.toBytes(key));


		scan.setFilter(filter);
		
		ResultScanner rs = table.getScanner(scan);
		for (Result result : rs) {
			if(i>limit) {
				break;
			}

			Map map=result.getNoVersionMap();
			Set set = map.keySet();
			Iterator it = set.iterator();
			while(it.hasNext()) {
				byte[] cfkey = (byte[])it.next();
				
				Map kvMap = (Map)map.get(cfkey);
				Set<Map.Entry> kvs = kvMap.entrySet();
				Iterator kvit = kvs.iterator();
				while(kvit.hasNext()) {
					Map.Entry<byte[],byte[]> me = (Map.Entry)kvit.next();
					searchre.add(Bytes.toString(me.getValue()));
				}
			}

			i++;
		}
		for(int j = 0 ; j <searchre.size();j+=3) {
			System.out.println();
			System.out.println(searchre.get(j + 1));
			System.out.println(searchre.get(j + 2));
		}
		
	}
}
