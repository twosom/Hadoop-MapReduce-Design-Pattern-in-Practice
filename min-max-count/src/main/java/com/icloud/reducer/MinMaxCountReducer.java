package com.icloud.reducer;

import com.icloud.writable.MinMaxCountTuple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

    private MinMaxCountTuple result = new MinMaxCountTuple();

    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context)
            throws IOException, InterruptedException {
        initialize();

        int sum = 0;
        for (MinMaxCountTuple value : values) {
            // 입력의 최솟값이 결과의 최솟값보다 작으면
            // 입력 최솟값을 결과 최솟값으로 설정
            if (result.getMin() == null || value.getMin().compareTo(result.getMin()) < 0) {
                result.setMin(value.getMin());
            }
            if (result.getMax() == null || value.getMax().compareTo(result.getMax()) > 0) {
                result.setMax(value.getMax());
            }
            //TODO
            // sum += 1 이 아닌 이유는
            // Combiner 를 거치기 때문
            // 결국 Combiner 를 거친 만큼 Row 갯수가 줄어듦.
            sum += (int) value.getCount();
        }

        result.setCount(sum);
        context.write(key, result);
    }

    private void initialize() {
        result.setMax(null);
        result.setMax(null);
        result.setCount(0);
    }
}
