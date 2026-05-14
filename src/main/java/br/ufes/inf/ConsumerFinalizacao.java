package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerFinalizacao {
    public static void main(String[] args) {
        KafkaConsumer<String, EventoFutebol> consumer = KafkaCreate.createConsumer("shots-group");

        String topic = "match-events-raw";
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, EventoFutebol> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, EventoFutebol> record : records) {
                    EventoFutebol evento = record.value();

                    if (evento == null || evento.getType() == null) continue;

                    if ("SHOT".equals(evento.getType().getName())) {
                        String matchId = record.key() != null ? record.key() : "Unknown_Game";
                        String time = (evento.getTeam() != null) ? evento.getTeam().getName() : "Unknown";

                        System.out.printf("[ALERTA | %s] Finalização do time '%s'!%n", matchId, time);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("ERRO (ConsumerFinalizacao): " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
