package com.icloud;

import com.icloud.mapper.WikipediaExtractor;
import com.icloud.reducer.Concatenator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaExtractorDriver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WikipediaExtractorDriver <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf(), "StackOverflow Posts Wikipedia Extractor");
        job.setJarByClass(getClass());
        job.setMapperClass(WikipediaExtractor.class);
        job.setCombinerClass(Concatenator.class);
        job.setReducerClass(Concatenator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WikipediaExtractorDriver(), args);
        System.exit(exitCode);
    }
}