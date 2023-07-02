package com.icloud.v1.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

import static com.icloud.MRDPUtils.transformXmlToMap;

public class MedianStdDevMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable outHour = new IntWritable();
    private IntWritable outCommentLength = new IntWritable();

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context)
            throws IOException, InterruptedException {
        Map<String, String> parsed = transformXmlToMap(value.toString());
        LocalDateTime creationDate = LocalDateTime.parse(parsed.get("CreationDate"));
        String text = parsed.get("Text");

        outHour.set(creationDate.getHour());
        outCommentLength.set(text.length());

        context.write(outHour, outCommentLength);
    }
}
