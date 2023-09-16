package com.icloud.mapper;

import com.icloud.model.User;
import com.icloud.parser.impl.UserParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class CountNumUsersByStateMapper extends Mapper<LongWritable, Group, NullWritable, NullWritable> {
    public static final String STATE_COUNTER_GROUP = "State";
    public static final String UNKNOWN_COUNTER = "UNKNOWN";
    public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";
    private UserParser parser = new UserParser();

    private List<String> states = Arrays.asList(
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    );

    @Override
    protected void map(LongWritable key, Group value, Context context)
            throws IOException, InterruptedException {
        User user = parser.parse(value);
        String location = user.getLocation();
        if (location != null && !location.isEmpty()) {
            String[] tokens = location.toUpperCase(Locale.ROOT).split(",");

            boolean unknown = true;

            for (String state : tokens) {
                state = state.trim();
                if (states.contains(state)) {
                    context.getCounter(STATE_COUNTER_GROUP, state).increment(1);
                    unknown = false;
                    break;
                }
            }

            if (unknown) {
                context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
            } else {
                context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER).increment(1);
            }
        }
    }
}
