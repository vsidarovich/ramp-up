package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class IpReducerTest {
    ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        IpReducer reducer = new IpReducer();
        reduceDriver = new ReduceDriver();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = Arrays.asList(new IntWritable(15), new IntWritable(15));
        reduceDriver.withInput(new Text("ip1"), values);
        reduceDriver.withOutput(new Text("ip1"), new Text("ip1,15.0,2"));
        reduceDriver.runTest();
    }

}
