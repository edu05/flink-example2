package org.myorg.quickstart;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static org.apache.flink.streaming.api.TimeCharacteristic.ProcessingTime;

public class AppWithKafka {

    public static final String PRODUCER_TOPIC = "my-first-topic";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1)
                .setMaxParallelism(1)
                .enableCheckpointing(5000)
                .setStateBackend(new FsStateBackend("file:///Users/edu/wordcount/store"));

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStreamTimeCharacteristic(ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "asdajsdasdzxdsdfsf");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        ObjectMapper objectMapper = new ObjectMapper();
        env.addSource(new FlinkKafkaConsumer<>(PRODUCER_TOPIC, new SimpleStringSchema(), properties))
                .map(s -> objectMapper.readValue(s, TimestampedWord.class))
                .keyBy(TimestampedWord::getWord)
                .process(new TimestampedWordCountingFunction())
                .print();

        env.execute();
    }
}