package com.icloud;

import com.icloud.mapper.MinMaxCountMapper;
import com.icloud.model.MinMaxCountTuple;
import com.icloud.reducer.MinMaxCountReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CommentsMinMaxCountDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CommentsMinMaxCountDriver <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf(), "StackOverflow Comment Min Max Count");
        job.setJarByClass(getClass());
        job.setMapperClass(MinMaxCountMapper.class);
        job.setCombinerClass(MinMaxCountReducer.class);
        job.setReducerClass(MinMaxCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CommentsMinMaxCountDriver(), args);
        System.exit(exitCode);
    }
}