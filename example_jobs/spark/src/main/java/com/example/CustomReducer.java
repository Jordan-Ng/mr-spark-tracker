package com.example;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;


public class CustomReducer extends Reducer <Text, DataTuple, Text, FloatTuple> {
    
    @Override
    protected void reduce(Text key, Iterable<DataTuple> values, Context context) throws IOException, InterruptedException {

        Integer total_usage = 0;
        Text device_name = new Text();
     
        for (DataTuple value : values){
            device_name = value.first;
            total_usage += Integer.parseInt(value.second.toString());
        }
        float daily_usage = (total_usage / (float) 86400000) * 100;
        context.write(key, new FloatTuple(device_name.toString(), daily_usage));
    }

}
