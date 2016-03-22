package com.epam.test;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

/**
 * Created by cloudera on 2/15/16.
 */
public class Runner {
    static String fileDirectory = new String ("hdfs://quickstart.cloudera:8020/user/cloudera/streaming/");

    public static void main(String... args) {

        SparkConf
                conf = new SparkConf()
                .setAppName("File streaming")
                .setMaster("local[4]")
                .set("spark.driver.allowMultipleContexts", "true");

        StreamingContext  ssc = new StreamingContext(conf, new Duration(5000L));
        DStream stream = ssc.textFileStream(fileDirectory);
        stream.print(10);

       // quickstart.cloudera#sthash.3Bx88CQi.dpuf
        /*stream.foreachRDD((v1, v2) -> {

        });
         stream.print(10);
        */



        ssc.start();
        ssc.awaitTermination();

    }
}
