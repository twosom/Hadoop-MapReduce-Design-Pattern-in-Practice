package com.icloud;

import com.icloud.model.Comment;
import com.icloud.parser.CommentParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;


public class CommentWordCount extends Configured implements Tool {


    public static class WordCountMapper extends Mapper<LongWritable, Group, Text, IntWritable> {

        private final CommentParser parser = new CommentParser();

        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Group value, Context context)
                throws IOException, InterruptedException {
            Comment comment = parser.parse(value);
            String text = comment.getText();
            if (text == null) {
                return;
            }

            text = text.toLowerCase(Locale.ROOT);

            text = text.replaceAll("'", ""); // 홑따옴표 제거
            text = text.replaceAll("[^a-zA-Z]", " "); // 나머지는 공백 처리

            StringTokenizer iter = new StringTokenizer(text);
            while (iter.hasMoreTokens()) {
                word.set(iter.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
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


        Job job = Job.getInstance(getConf(), "Comment Word Count Application");
        job.setJarByClass(getClass());

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        // 출력 설정
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Parquet 파일 인풋 설정
        ParquetInputFormat.setInputPaths(job, new Path(args[0]));
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        job.setInputFormatClass(ParquetInputFormat.class);

        // File 아웃풋 설정
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CommentWordCount(), args);
        System.exit(exitCode);
    }
}
