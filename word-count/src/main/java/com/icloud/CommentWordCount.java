package com.icloud;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

public class CommentWordCount extends Configured implements Tool {

    public static class WordCountMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key,
                           Text value,
                           Context context)
                throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
            String txt = parsed.get("Text");
            if (txt == null) return;
            txt = StringEscapeUtils.unescapeHtml4(txt.toLowerCase());

            txt = txt.replaceAll("'", "")
                    .replaceAll("[^a-zA-Z]", " ");

            StringTokenizer itr = new StringTokenizer(txt);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key,
                              Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            int sum = StreamSupport.stream(values.spliterator(), false)
                    .mapToInt(IntWritable::get)
                    .sum();
            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CommentWordCount <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf(), "StackOverflow Comment Word Count");
        job.setJarByClass(getClass());
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CommentWordCount(), args);
        System.exit(exitCode);
    }
}
