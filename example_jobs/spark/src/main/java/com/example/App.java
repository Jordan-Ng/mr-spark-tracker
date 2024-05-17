package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.HandleOpt.Data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.*;


public class App 
{
    public static void main( String[] args ) throws Exception
    {
        String urlString = "http://localhost:8000/update/mr/";
        HttpClient client = HttpClient.newHttpClient();
        
        Configuration conf = new Configuration();
        conf.set("yarn.resourcemanage.adress", "localhost:8050");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        
        conf.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
        conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
        
        Job job = Job.getInstance(conf, "IOT Usage Summary");
        
        job.setJarByClass(App.class);
        
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataTuple.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatTuple.class);
        
        FileInputFormat.addInputPath(job, new Path("input_large"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
        System.out.println("============================== START OF JOB ==================================");
        Integer jobStatus = 0;
        try{
            HttpRequest before_request = HttpRequest.newBuilder(URI.create(urlString + (System.currentTimeMillis() ))).header("accept", "*").version(HttpClient.Version.HTTP_1_1).POST(HttpRequest.BodyPublishers.noBody()).build();
            
            client.send(before_request, BodyHandlers.ofString());
            jobStatus = job.waitForCompletion(true) ? 0 :1;
            HttpRequest after_request = HttpRequest.newBuilder(URI.create(urlString + (System.currentTimeMillis() ))).header("accept", "*").version(HttpClient.Version.HTTP_1_1).POST(HttpRequest.BodyPublishers.noBody()).build();
            
            client.send(after_request, BodyHandlers.ofString());

        }catch (Exception e){
            e.printStackTrace();
        }   
        System.out.println("============================== END OF JOB ==================================");

        System.out.println("Exiting program with status: " + jobStatus);
        System.exit(jobStatus);
    }
}
