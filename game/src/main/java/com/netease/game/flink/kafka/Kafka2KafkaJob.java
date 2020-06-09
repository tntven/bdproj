package com.netease.game.flink.kafka;

import com.netease.game.flink.util.FlinkUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 *
 * 配置样例
 * --kafka.source.topic kafka-test --kafka.source.group.id test --kafka.source.bootstrap.servers localhost:9092
 * --kafka.sink.topic kafka-sink --kafka.sink.bootstrap.servers localhost:9092
 *
 * 写入Kafka
 * zcat merged.gz | head -100 | kafka-console-producer.sh --broker-list localhost:9092 --topic kafka-test
 */
public class Kafka2KafkaJob {
    public static void main(String[] args) throws Exception {

        ParameterTool pt = ParameterTool.fromArgs(args);
        if (pt.get("config") != null) {
            pt = ParameterTool.fromPropertiesFile(pt.get("config"));
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        Properties srcProp = FlinkUtil.prepareKafkaProperties(pt, "kafka.source");
        String sourceTopic = pt.get("kafka.source.topic");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(sourceTopic, new SimpleStringSchema(), srcProp);
        consumer.setStartFromEarliest();
        DataStreamSource<String> sourceStream = env.addSource(consumer);

        /*
        FlinkKafkaConsumer<String> consumer2 = new FlinkKafkaConsumer<String>(
                sourceTopic,
                new KafkaDeserializationSchema<String>() {
                    private boolean end;

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return end;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        return new String(record.key(), StandardCharsets.UTF_8)+ "_" + new String(record.value(), StandardCharsets.UTF_8);
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        // return Types.STRING; // flink 内置支持类型
                        // TypeInformation.of(xxx.class)
                    }
                },
                srcProp
        );

         */

        // sink
        Properties destProp = FlinkUtil.prepareKafkaProperties(pt, "kafka.sink");
        String sinkTopic = pt.get("kafka.sink.topic");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                sinkTopic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long timestamp) {
                        // 可以根据元素来发送到不同topic
                        // return new ProducerRecord<>(getTopicName(s), null, timestamp, null, s.getBytes(StandardCharsets.UTF_8));
                        return new ProducerRecord<>(sinkTopic, null, timestamp, null, s.getBytes(StandardCharsets.UTF_8));
                    }
                },
                destProp,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        sourceStream.addSink(producer);

        env.execute("Kafka Job");
    }
}
