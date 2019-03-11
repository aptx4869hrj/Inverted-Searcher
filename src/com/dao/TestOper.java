package com.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.huaban.analysis.jieba.JiebaSegmenter;

public class TestOper {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "C:\\Users\\HRJ\\hadoop-2.7.7");
		
		//创建配置对象
		Configuration config = HBaseConfiguration.create();
		//配置zookeeper集群
		config.set("hbase.zookeeper.quorum", "172.17.11.141,172.17.11.144,172.17.11.145");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		//配置hmaster地址
		config.set("hbase.master", "172.17.11.231:16000");
		System.out.println("111");
		try {
			//获取hbase数据库的链接
			Connection connection = ConnectionFactory.createConnection(config);
			//根据表名获取表
			Table table = connection.getTable(TableName.valueOf("kafkaData"));
			
			//q1(table);
			//createTable(connection);
			//insert(table);
			
			scantable(table,1000);
			
			table.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void scantable(Table table,int limit) throws IOException {
		int i=0;
		Scan scan = new Scan();
		
		Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("RowKey99")));
		//Filter filter = new PrefixFilter(Bytes.toBytes("01"));
		//Filter filter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("沈阳"));

		scan.setFilter(filter);
		
		ResultScanner rs = table.getScanner(scan);
		for (Result result : rs) {
			if(i>limit) {
				break;
			}
			//byte[] jn=result.getValue(Bytes.toBytes("stuno"), Bytes.toBytes("year"));
			//System.out.println(Bytes.toString(jn));
	        
			System.out.print("RowKey:["+Bytes.toString(result.getRow())+"]");
			System.out.print("\t");

			//Map<byte[],Map<byte[],byte[]>>
			Map map=result.getNoVersionMap();
			Set set = map.keySet();
			Iterator it = set.iterator();
			while(it.hasNext()) {
				byte[] cfkey = (byte[])it.next();
				System.out.print(Bytes.toString(cfkey)+":[");
				
				Map kvMap = (Map)map.get(cfkey);
				Set<Map.Entry> kvs = kvMap.entrySet();
				Iterator kvit = kvs.iterator();
				while(kvit.hasNext()) {
					Map.Entry<byte[],byte[]> me = (Map.Entry)kvit.next();
					System.out.print(Bytes.toString(me.getKey())+":"+Bytes.toString(me.getValue())+",");
					
					//String text = Bytes.toString(me.getValue());
			        //JiebaSegmenter segmenter = new JiebaSegmenter();
			        //System.out.println(segmenter.sentenceProcess(text));
				}
				System.out.print("]\t");

			}
			
			System.out.println("");
			i++;
		}
	}

	private static void insert(Table table) throws IOException {	
		String filepath = "C:\\Users\\HRJ\\Documents\\Python Scripts\\spdier\\new1";
		File file = new File(filepath); 
        if (!file.isDirectory()) { 
               // System.out.println("文件"); 
                System.out.println("path=" + file.getPath()); 
                System.out.println("absolutepath=" + file.getAbsolutePath()); 
                System.out.println("name=" + file.getName()); 
        } else if (file.isDirectory()) { 
                //System.out.println("文件夹"); 
                String[] filelist = file.list(); 
                for (int i = 0; i < filelist.length; i++) { 
                        File readfile = new File(filepath + "\\" + filelist[i]); 
                        if (!readfile.isDirectory()) {
                        	Put put = new Put(Bytes.toBytes("ROW"+i));
                        	FileInputStream inputStream = new FileInputStream(readfile.getPath());
                    		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    		String str = bufferedReader.readLine();
                    		String url = bufferedReader.readLine();	                    			
                    		//close
                    		inputStream.close();
                    		bufferedReader.close();

                                put.addColumn(Bytes.toBytes("title"), Bytes.toBytes("title"), Bytes.toBytes(str));
                                put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("url"), Bytes.toBytes(url));
                                table.put(put);
                        } 
                }
        }
	}

	private static void createTable(Connection connection) throws IOException {
		//创建数据库管理对象
		Admin admin = connection.getAdmin();// hbase表管理类
		//创建表对象
//		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("InvertedData"));
		HTableDescriptor htd1 = new HTableDescriptor(TableName.valueOf("urlData"));
		//创建列族对象
		HColumnDescriptor kehcd = new HColumnDescriptor("key");
		HColumnDescriptor tihcd = new HColumnDescriptor("title");
		HColumnDescriptor urlhcd = new HColumnDescriptor("url");


		//把列族加入表
		htd1.addFamily(kehcd);
		htd1.addFamily(tihcd);
		htd1.addFamily(urlhcd);
//		htd.addFamily(tihcd);
//		htd.addFamily(urlhcd);

		//创建表
//		admin.createTable(htd);
		admin.createTable(htd1);
		admin.close();
	}
}
