package com.example;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class CustomMapper extends Mapper <LongWritable, Text, Text, DataTuple> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String [] data = value.toString().split(",");
        String mac_address = data[1];
        Integer elapsed = Integer.parseInt(data[4]) - Integer.parseInt(data[3]);
        context.write(new Text(mac_address), new DataTuple(data[2], elapsed));

    }
}
