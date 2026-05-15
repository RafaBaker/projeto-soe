package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerPressao {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pression-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventoTaticoDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, EventoTatico> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("match-insight"));

        try {
            while (true) {
                ConsumerRecords<String, EventoTatico> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, EventoTatico> record : records) {
                    EventoTatico evento = record.value();
                    if (evento != null) {
                        System.out.printf("[INSIGHT | %s ] %s detectada da equipe '%s'!%n",
                                evento.getMatchId(), evento.getInsight(), evento.getTeam());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}