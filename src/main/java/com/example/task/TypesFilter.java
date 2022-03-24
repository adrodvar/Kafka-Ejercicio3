package com.example.task;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TypesFilter {

    static Logger log = LoggerFactory.getLogger(TypesFilter.class);
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
        KStream<String, String> types = builder.stream(inputTopic);
        // processing
        KStream<String, String> carsTypes = types
                .filter((key, value) -> value.contains("Toyota")) // transformación 1
                .peek((key, value) -> log.info("k {} --- v {}", key, value))
                .filter((key, value) -> value.contains("Jeep")) // transformación 2
                .peek((key, value) -> log.info("k {} --- v {}", key, value))
                .mapValues(value -> value.substring(value.indexOf("-") + 1))// transformación 3
                .filter((key, value) -> value.contains("volkswagen"))
                .mapValues(value -> value.substring(value.indexOf("-") + 1))// transformación 4
                .filter((key, value) -> value.contains("BMW"));;
        // produce
        carsTypes.to(outputTopic);
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}