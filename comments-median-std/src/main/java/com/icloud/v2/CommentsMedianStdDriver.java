package com.icloud.v2;

import com.icloud.model.MedianStdDevTuple;
import com.icloud.v2.combiner.MedianStdDevCombiner;
import com.icloud.v2.mapper.MedianStdDevMapper;
import com.icloud.v2.reducer.MedianStdDevReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CommentsMedianStdDriver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CommentsMedianStdDriver v2 <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf(), "StackOverflow Comment Median Std v2");
        job.setJarByClass(getClass());
        job.setMapperClass(MedianStdDevMapper.class);
        job.setCombinerClass(MedianStdDevCombiner.class);
        job.setReducerClass(MedianStdDevReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SortedMapWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedianStdDevTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CommentsMedianStdDriver(), args);
        System.exit(exitCode);
    }
}