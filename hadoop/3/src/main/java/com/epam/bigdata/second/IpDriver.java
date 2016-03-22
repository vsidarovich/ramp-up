package com.epam.bigdata.second;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class IpDriver extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(IpDriver.class);

    public static void main(String... args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new IpDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        LOG.info("IP Job has started");

        job.setJarByClass(getClass());

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);

        job.setMapperClass(IpMapper.class);
        job.setReducerClass(IpReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setCompressOutput(job, true);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        boolean isWaitForCompletion = job.waitForCompletion(true);

        CounterGroup group = job.getCounters().getGroup("browser");
        long mozillaCount = group.getUnderlyingGroup().findCounter("Mozilla").getValue();
        long operaCount = group.getUnderlyingGroup().findCounter("Opera").getValue();
        long otherCount = group.getUnderlyingGroup().findCounter("Other").getValue();


        System.out.println("Mozilla browser users count = " + mozillaCount);
        System.out.println("Opera browser users count = " + operaCount);
        System.out.println("Other browser users count = " + otherCount);

        LOG.info("IP Job has ended");
        return isWaitForCompletion ? 0 : 1;
    }
}
