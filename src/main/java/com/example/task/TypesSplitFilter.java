package com.example.task;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TypesSplitFilter {

    static Logger log = LoggerFactory.getLogger(TypesSplitFilter.class);
    static String kafkaUrl = "https://localhost:9092";
    static String appId = "task-streams-cars-types";
    static String inputTopic = "cars";
    static String outputTopic = "cars-types";

    public static void main(String[] args) {
        AdminUtils.deleteTypesTopic();
        Producer.generateTypesTopic();
        Properties config = new Properties();
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        StreamsBuilder builder = new StreamsBuilder();
        // consume
        KStream<String, String> titles = builder.stream(inputTopic);
        // processing and produce
        titles.split()
                .branch((key, value) -> value.toLowerCase().contains("Toyota"), Branched.withConsumer(kstream -> kstream.to("types-camper")))
                .branch((key, value) -> value.toLowerCase().contains("Jeep"), Branched.withConsumer(kstream -> kstream.to("types-camper")))
                .branch((key, value) -> value.toLowerCase().contains("volkswagen"), Branched.withConsumer(kstream -> kstream.to("types-familiar")))
                .defaultBranch(Branched.withConsumer(kstream -> kstream.to("types-other")));
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}