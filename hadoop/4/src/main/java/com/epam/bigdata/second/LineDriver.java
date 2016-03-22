package com.epam.bigdata.second;

import com.epam.bigdata.second.model.RowNumberWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LineDriver extends Configured implements Tool {
    public final static byte COUNTER_MARKER = (byte) 'T';
    public final static byte VALUE_MARKER = (byte) 'W';

    public static void main(String... args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new LineDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());

        job.setGroupingComparatorClass(LineComparator.class);
        job.setPartitionerClass(LinePartitioner.class);

        job.setMapperClass(LineMapper.class);
        job.setMapOutputKeyClass(ByteWritable.class);
        job.setMapOutputValueClass(RowNumberWritable.class);

        job.setReducerClass(LineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, input);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}
