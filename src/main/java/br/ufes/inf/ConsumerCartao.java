package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.StreamSupport;

public class ConsumerCartao {
    public static void main(String[] args) {
        KafkaConsumer<String, EventoFutebol> consumer = KafkaCreate.createConsumer("cards-group");

        String topic = "match-events-raw";
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, EventoFutebol> records = consumer.poll(Duration.ofMillis(100));

                StreamSupport.stream(records.spliterator(), false)
                        .filter(record -> record.value() != null && record.value().getType() != null)
                        .filter(record -> "CARD".equals(record.value().getType().getName()))
                        .forEach(record -> {
                            EventoFutebol evento = record.value();
                            String matchId = record.key() != null ? record.key() : "Unknown_Game";
                            String time = (evento.getTeam() != null ? evento.getTeam().getName() : "Unknown");

                            System.out.printf("[AVISO | %s | Tempo: %s] Cartão aplicado para '%s'!%n",
                                    matchId, evento.retornaTempoRegulamentar(), time);
                        });
            }
        } catch (Exception e) {
            System.err.println("ERRO (ConsumerCartao): " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}