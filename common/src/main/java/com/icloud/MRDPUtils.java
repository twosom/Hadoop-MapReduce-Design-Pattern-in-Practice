package com.icloud;

import java.util.HashMap;
import java.util.Map;

public class MRDPUtils {
    public static Map<String, String> transformXmlToMap(final String xml) {
        Map<String, String> map = new HashMap<>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String value = tokens[i + 1];

                map.put(key.substring(0, key.length() - 1), value);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }
        return map;
    }
}
