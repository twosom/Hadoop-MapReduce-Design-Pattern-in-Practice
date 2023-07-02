package com.icloud.mapper;

import com.icloud.model.MinMaxCountTuple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static com.icloud.MRDPUtils.transformXmlToMap;

public class MinMaxCountMapper extends Mapper<LongWritable, Text, Text, MinMaxCountTuple> {

    private Text outUserId = new Text();
    private MinMaxCountTuple outTuple = new MinMaxCountTuple();
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context)
            throws IOException, InterruptedException {
        Map<String, String> parsed = transformXmlToMap(value.toString());
        Date creationDate = parse(parsed.get("CreationDate"));
        if (creationDate == null || parsed.get("UserId") == null) return;
        outUserId.set(parsed.get("UserId"));

        outTuple.setMin(creationDate);
        outTuple.setMax(creationDate);
        outTuple.setCount(1);

        context.write(outUserId, outTuple);
    }

    private Date parse(String createDate) {
        try {
            if (createDate == null) return null;
            return sdf.parse(createDate);
        } catch (ParseException e) {
            return null;
        }
    }
}
