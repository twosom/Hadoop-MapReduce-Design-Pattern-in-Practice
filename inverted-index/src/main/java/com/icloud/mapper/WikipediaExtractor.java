package com.icloud.mapper;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import static com.icloud.MRDPUtils.transformXmlToMap;

public class WikipediaExtractor extends Mapper<LongWritable, Text, Text, Text> {
    private Text link = new Text();
    private Text outKey = new Text();

    Pattern HOSTNAME_MATCHER = Pattern.compile("\\.?wikipedia\\.org/.*",
            Pattern.CASE_INSENSITIVE);

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context)
            throws IOException, InterruptedException {
        Map<String, String> parsed = transformXmlToMap(value.toString());
        String text = parsed.get("Body");
        String postType = parsed.get("PostTypeId");
        String rowId = parsed.get("Id");
        if (rowId == null || text == null || (postType != null && postType.equals("1"))) return;

        text = StringEscapeUtils.unescapeHtml4(text.toLowerCase());
        String wikipediaURL = getWikipediaURL(text);
        if (wikipediaURL == null) return;

        link.set(wikipediaURL);
        outKey.set(rowId);
        context.write(link, outKey);
    }

    private String getWikipediaURL(String body) {
        Document doc = Jsoup.parse(body);
        return doc.select("a").stream()
                .filter(link -> link.hasAttr("href") && link.attr("href").length() != 0)
                .map(link -> link.attr("href"))
                .filter(url -> HOSTNAME_MATCHER.matcher(url).find())
                .findFirst()
                .orElse(null);
    }
}
