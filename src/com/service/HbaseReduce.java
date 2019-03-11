package com.service;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jruby.RubyProcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseReduce extends TableReducer<Text,Text,Text> {
    private int id=1;

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context)
            throws IOException, InterruptedException {


        List<String>urls=new ArrayList<>();
        List<String>titles=new ArrayList<>();
        List<Integer>counts=new ArrayList<>();
        for(Text text:values){
            String urlAndTitle=String.valueOf(text);
            String url1 = urlAndTitle.split(":")[0];
            String url2 = urlAndTitle.split(":")[1];
            String title = urlAndTitle.split(":")[2];
            
            String url = url1 + ":" + url2;
            System.out.println(key);
            System.out.println(urlAndTitle);
            System.out.println(url);
            System.out.println(title);
            
            Put put = new Put(Bytes.toBytes(String.valueOf(id)));

            System.out.println(id);
            
            put.addColumn(Bytes.toBytes("key"), Bytes.toBytes("key"), Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("title"), Bytes.toBytes("title"), Bytes.toBytes(title));
            put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("url"), Bytes.toBytes(url));
            
            id ++;
            context.write(new Text(String.valueOf(id)),put);
        }
    }
}

