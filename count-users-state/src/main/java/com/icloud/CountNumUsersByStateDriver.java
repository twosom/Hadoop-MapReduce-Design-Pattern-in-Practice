package com.icloud;

import com.icloud.mapper.CountNumUsersByStateMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class CountNumUsersByStateDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s <in> <out>", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf(), "Count Number User State Application");
        job.setJarByClass(getClass());

        job.setMapperClass(CountNumUsersByStateMapper.class);

        // Parquet 파일 인풋 설정
        ParquetInputFormat.setInputPaths(job, new Path(args[0]));
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        job.setInputFormatClass(ParquetInputFormat.class);

        // File 아웃풋 설정
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        if (exitCode == 0) {
            for (Counter counter : job.getCounters().getGroup(CountNumUsersByStateMapper.STATE_COUNTER_GROUP)) {
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
            }
        }

        // 빈 출력 디렉토리 삭제
        FileSystem.get(getConf()).delete(outputPath, true);
        return exitCode;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CountNumUsersByStateDriver(), args);
        System.exit(exitCode);
    }
}