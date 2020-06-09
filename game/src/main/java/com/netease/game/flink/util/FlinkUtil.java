package com.netease.game.flink.util;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

public class FlinkUtil {

    public static Properties prepareKafkaProperties(ParameterTool pt, String prefix) {
        Properties properties = new Properties();
        Properties ptProp = pt.getProperties();
        for (Object objKey : ptProp.keySet()) {
            String key = objKey.toString();
            if (key.startsWith(prefix)) {
                String value = ptProp.getProperty(key);
                properties.setProperty(key.substring(prefix.length() - 1), value);
            }
        }
        return properties;
    }
}
