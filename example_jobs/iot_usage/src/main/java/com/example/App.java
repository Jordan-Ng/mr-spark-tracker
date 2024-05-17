package com.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.DataOutputStream;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.URI;
import java.net.HttpURLConnection;
import java.net.URL;

public class App 
{
    public static void main( String[] args )
    {
        SparkConf  sparkConf = new SparkConf().setAppName("IOT usage").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> iotRDD = sparkContext.textFile("input/IOT_large.csv");
        String urlString = "http://localhost:8000/update/spark/";

        HttpClient client = HttpClient.newHttpClient();
      

        JavaPairRDD<String, Tuple2<String, Integer>> iotFormattedPairRDD = iotRDD.mapToPair(
            new PairFunction<String, String, Tuple2<String, Integer>>() {
                public Tuple2<String, Tuple2<String, Integer>> call(String s) throws Exception {
                    String [] data = s.split(",");
                    String mac_address = data[1];
                    Integer elapsed = Integer.parseInt(data[4]) - Integer.parseInt(data[3]);
                    return new Tuple2(mac_address, new Tuple2(data[2], elapsed));
                }
            }
        );

        // reduce by key
        JavaPairRDD<String, Tuple2<String,Integer>> iotTotalUsageRDD = iotFormattedPairRDD.reduceByKey(
            new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>(){
                public Tuple2<String,Integer> call(Tuple2<String, Integer> t1, Tuple2<String,Integer> t2){
                    return new Tuple2(t1._1, t1._2 + t2._2);
                }
            }
        );
        
        // map to calculate usage
        JavaRDD<Tuple2<String, Tuple2<String, Float>>> iotPercentageUsageRDD = iotTotalUsageRDD.map(
            new Function<Tuple2<String, Tuple2<String,Integer>>, Tuple2<String, Tuple2<String,Float>>>(){
                public Tuple2<String, Tuple2<String,Float>> call(Tuple2<String, Tuple2<String, Integer>> data){
                    Float percentage = (data._2._2 / (float) 86400000) * 100;
                    return new Tuple2(data._1, new Tuple2(data._2._1, percentage));
                }
            }
        );

       
        HttpRequest initial_request = HttpRequest.newBuilder(URI.create(urlString + (System.currentTimeMillis() ))).header("accept", "*").version(HttpClient.Version.HTTP_1_1).POST(HttpRequest.BodyPublishers.noBody()).build();
        
        try{
            client.send(initial_request, BodyHandlers.ofString());
        }
        catch (Exception e){
            e.printStackTrace();
        }

        iotPercentageUsageRDD.saveAsTextFile("output4");

        HttpRequest final_request = HttpRequest.newBuilder(URI.create(urlString + (System.currentTimeMillis() ))).header("accept", "*").version(HttpClient.Version.HTTP_1_1).POST(HttpRequest.BodyPublishers.noBody()).build();
        
        try{
            client.send(final_request, BodyHandlers.ofString());
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
