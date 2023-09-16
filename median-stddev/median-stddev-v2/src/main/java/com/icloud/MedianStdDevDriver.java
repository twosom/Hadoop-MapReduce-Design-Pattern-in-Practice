package com.icloud;

import com.icloud.combiner.MedianStdDevCombiner;
import com.icloud.mapper.MedianStdDevMapper;
import com.icloud.reducer.MedianStdDevReducer;
import com.icloud.writable.MedianStdDevTuple;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class MedianStdDevDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s <in> <out>", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf(), "Comment Median StdDev Application V2");
        job.setJarByClass(getClass());

        job.setMapperClass(MedianStdDevMapper.class);
        job.setCombinerClass(MedianStdDevCombiner.class);
        job.setReducerClass(MedianStdDevReducer.class);

        // mapper output
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SortedMapWritable.class);

        //reducer output
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedianStdDevTuple.class);

        // Parquet 파일 인풋 설정
        ParquetInputFormat.setInputPaths(job, new Path(args[0]));
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        job.setInputFormatClass(ParquetInputFormat.class);

        // File 아웃풋 설정
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MedianStdDevDriver(), args);
        System.exit(exitCode);
    }
}
