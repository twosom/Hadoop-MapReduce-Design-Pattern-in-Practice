package com.icloud;

import com.icloud.mapper.AverageMapper;
import com.icloud.reducer.AverageReducer;
import com.icloud.writable.CountAverageTuple;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class CommentAverageDriver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s <in> <out>", getClass());
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf(), "Comment Average Application");
        job.setJarByClass(getClass());

        job.setMapperClass(AverageMapper.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);

        // 출력 설정
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(CountAverageTuple.class);

        // Parquet 파일 인풋 설정
        ParquetInputFormat.setInputPaths(job, new Path(args[0]));
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        job.setInputFormatClass(ParquetInputFormat.class);

        // File 아웃풋 설정
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CommentAverageDriver(), args);
        System.exit(exitCode);
    }
}