package com.icloud.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Concatenator extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, // key = wikipedia url
                          Iterable<Text> values, // value = [rowId]
                          Context context)
            throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Text rowId : values) {
            if (first) first = false;
            else sb.append(" ");
            sb.append(rowId.toString());
        }
        result.set(sb.toString());
        context.write(key, result);
    }
}
