package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.StreamSupport;

public class ConsumerFinalizacao {
    public static void main(String[] args) {
        KafkaConsumer<String, EventoFutebol> consumer = KafkaCreate.createConsumer("shots-group");

        String topic = "match-events-raw";
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, EventoFutebol> records = consumer.poll(Duration.ofMillis(100));

                StreamSupport.stream(records.spliterator(), false)
                        .filter(record -> record.value() != null && record.value().getType() != null)
                        .filter(record -> "SHOT".equals(record.value().getType().getName()))
                        .forEach(record -> {
                            EventoFutebol evento = record.value();
                            String matchId = record.key() != null ? record.key() : "Unknown_Game";
                            String time = (evento.getTeam() != null) ? evento.getTeam().getName() : "Unknown";

                            System.out.printf("[ALERTA | %s | Tempo: %s] Finalização do time '%s'!%n",
                                    matchId, evento.retornaTempoRegulamentar(), time);
                        });
            }
        } catch (Exception e) {
            System.err.println("ERRO (ConsumerFinalizacao): " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}