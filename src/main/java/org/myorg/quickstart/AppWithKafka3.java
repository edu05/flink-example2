package org.myorg.quickstart;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.file.sink.compactor.SimpleStringDecoder;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.apache.flink.streaming.api.TimeCharacteristic.ProcessingTime;

public class AppWithKafka3 {

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

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperties(properties)
                .setTopics(PRODUCER_TOPIC)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forMonotonousTimestamps();
        watermarkStrategy.withTimestampAssigner((event, timestamp) -> Instant.now().toEpochMilli());
        watermarkStrategy.withIdleness(Duration.ofSeconds(10));

        env.fromSource(kafkaSource, watermarkStrategy, "paco", Types.STRING)
                .map(s -> objectMapper.readValue(s, TimestampedWord.class))
                .keyBy(TimestampedWord::getWord)
                .process(new TimestampedWordCountingFunction())
                .print();

        env.execute();
    }
}