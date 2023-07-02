package com.icloud.mapper;

import com.icloud.model.CountAverageTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

import static com.icloud.MRDPUtils.transformXmlToMap;


public class AverageMapper extends Mapper<LongWritable, Text, IntWritable, CountAverageTuple> {

    private IntWritable outHour = new IntWritable();
    private CountAverageTuple outCountAverage = new CountAverageTuple();

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context)
            throws IOException, InterruptedException {
        final Map<String, String> parsed = transformXmlToMap(value.toString());

        LocalDateTime creationDate = parse(parsed.get("CreationDate"));
        outHour.set(creationDate.getHour());

        outCountAverage.setCount(1);
        String text = parsed.get("Text");
        outCountAverage.setAverage(text.length());

        context.write(outHour, outCountAverage);
    }

    private LocalDateTime parse(String dateString) {
        return LocalDateTime.parse(dateString);
    }
}
