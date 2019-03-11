package com.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Driver{
	public static final String tableName2 = "urlData";
	
    public static void main(String[] args) throws Exception {
    	
    	System.setProperty("hadoop.home.dir", "C:\\Users\\HRJ\\hadoop-2.7.7");
        Scanner scaner=new Scanner(System.in);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.17.11.141,172.17.11.144,172.17.11.145");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "172.17.11.231:16000");

        Job job = Job.getInstance(conf,"Driver");
        job.setJarByClass(Driver.class);

        List<Scan> list = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setStartRow("ROW0".getBytes());
        scan.setStopRow("ROW99".getBytes());
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "InvertedData".getBytes());
        list.add(scan);

        TableMapReduceUtil.initTableMapperJob(list,HbaseMap.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(tableName2, HbaseReduce.class, job);
       // TableMapReduceUtil.initTableReducerJob("InvertedIndexData", HbaseReduce.class, job);


        System.exit(job.waitForCompletion(true)==true?0:1);
    }
}

