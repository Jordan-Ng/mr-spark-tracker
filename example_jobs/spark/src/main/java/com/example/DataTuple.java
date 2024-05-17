package com.example;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class DataTuple implements WritableComparable<DataTuple> {
    public Text first;
    public IntWritable second;

    public DataTuple() {
        this.first = new Text();
        this.second = new IntWritable();
    }

    public DataTuple(String first, Integer second){
        try {
            this.first = new Text(first);
            this.second = new IntWritable(second);
        }catch(Exception e){
            System.out.println(e.getCause());
        }
    }

    // @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    public int compareTo(DataTuple dt2) {
        int cmp = first.compareTo(dt2.first);

        if (cmp != 0){
            return cmp;
        }

        return second.compareTo(dt2.second);
    }

    public void write(DataOutput out) throws IOException{
        first.write(out);
        second.write(out);
    }

    public Text getFirst(){
        return this.first;
    }

    public String toString() {
        return new String(first + "," + second.toString() );
    }

}
