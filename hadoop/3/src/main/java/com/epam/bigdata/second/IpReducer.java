package com.epam.bigdata.second;

import com.epam.bigdata.second.model.IpWrittableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class IpReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {
        int count = 0;
        int sum = 0;

        for (IntWritable value : values) {
            count++;
            sum += value.get();
        }
        double average = sum / count;
        IpWrittableComparable result = new IpWrittableComparable(average, count);
        Text textResult = new Text(key + "," + result);
        context.write(key, textResult);
    }
}
