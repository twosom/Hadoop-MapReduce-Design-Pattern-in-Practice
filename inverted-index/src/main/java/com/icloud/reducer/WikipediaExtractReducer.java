package com.icloud.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.joining;

public class WikipediaExtractReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        boolean first = true;
        String ids = StreamSupport.stream(values.spliterator(), false)
                .map(Text::toString)
                .collect(joining(" "));
        result.set(ids);
        context.write(key, result);
    }
}
