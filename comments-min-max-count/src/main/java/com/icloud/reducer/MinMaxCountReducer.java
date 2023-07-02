package com.icloud.reducer;

import com.icloud.model.MinMaxCountTuple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

    private MinMaxCountTuple result = new MinMaxCountTuple();

    @Override
    protected void reduce(Text key,
                          Iterable<MinMaxCountTuple> values,
                          Context context)
            throws IOException, InterruptedException {
        init();
        int sum = 0;
        for (final MinMaxCountTuple value : values) {
            if (isLowerThan(value)) result.setMin(value.getMin());
            if (isGreaterThan(value)) result.setMax(value.getMax());
            sum += value.getCount();
        }
        result.setCount(sum);
        context.write(key, result);
    }

    private boolean isGreaterThan(MinMaxCountTuple value) {
        return result.getMax() == null || result.getMax().compareTo(value.getMin()) > 0;
    }

    private boolean isLowerThan(final MinMaxCountTuple value) {
        return result.getMin() == null || value.getMin().compareTo(result.getMin()) < 0;
    }

    private void init() {
        result.setMax(null);
        result.setMax(null);
        result.setCount(0);
    }
}
