package com.example;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// public class FloatTuple extends DataTuple {
public class FloatTuple implements WritableComparable<FloatTuple> {
    public Text first;
    public FloatWritable second;

    public FloatTuple() {
        this.first = new Text();
        this.second = new FloatWritable();
    }

    public FloatTuple(String first, Float second){
        try {
            this.first = new Text(first);
            this.second = new FloatWritable(second);
        }catch(Exception e){
            System.out.println(e.getCause());
        }
    }

    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    public int compareTo(FloatTuple dt2) {
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
