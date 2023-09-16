package com.icloud.mapper;

import com.icloud.model.Post;
import com.icloud.parser.impl.PostParser;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.Locale;
import java.util.regex.Pattern;

public class WikipediaExtractMapper extends Mapper<LongWritable, Group, Text, Text> {

    Pattern HOSTNAME_MATCHER = Pattern.compile("\\.?wikipedia\\.org/.*",
            Pattern.CASE_INSENSITIVE);
    private Text link = new Text();
    private Text outKey = new Text();
    private PostParser parser = new PostParser();

    @Override
    protected void map(LongWritable key, Group value, Context context)
            throws IOException, InterruptedException {
        Post post = parser.parse(value);
        String body = post.getBody();
        Integer postId = post.getId();

        if (body == null || post.isPost()) {
            return;
        }

        body = StringEscapeUtils.unescapeHtml4(body.toLowerCase(Locale.ROOT));

        String wikipediaURL = getWikipediaURL(body);
        if (wikipediaURL == null) return;
        link.set(wikipediaURL);
        outKey.set(String.valueOf(postId));
        context.write(link, outKey);
    }

    private String getWikipediaURL(String body) {
        Document doc = Jsoup.parse(body);
        return doc.select("a").stream()
                .filter(link -> link.hasAttr("href") && !link.attr("href").isEmpty())
                .map(link -> link.attr("href"))
                .filter(url -> HOSTNAME_MATCHER.matcher(url).find())
                .findFirst()
                .orElse(null);
    }
}
