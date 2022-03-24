package com.example.task;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class Producer {

    static String kafkaUrl = "http://localhost:9092";
    static String typesCarTopic = "cars";
    static String titlesCarsTopic = "cars-types";
    static List<String> topicsToDelete = List.of(typesCarTopic, titlesCarsTopic);

    public static void generateTypesTopic() {
        Properties config = new Properties();
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        List<String> types = List.of(
                "cars types familiar-volkswagen",
                "cars types familiar-BMW",
                "cars types coupe Ferrari",
                "cars types coupe McLaren",
                "cars types Camper Toyota",
                "cars types Camper Jeep",
                "cars types Camper Nissan",
                "cars types Camper LandRover",
                "cars types Camper Waz");

        for (int i = 0; i < types.size(); i++)
            producer.send(new ProducerRecord<>(typesCarTopic, "cars" + i, types.get(i)));
    }


}
