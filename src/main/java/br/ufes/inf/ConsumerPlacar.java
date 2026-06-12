package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

public class ConsumerPlacar {
    public static void main(String[] args) {
        KafkaConsumer<String, EventoFutebol> consumer = KafkaCreate.createConsumer("placar-group");

        String topic = "match-events-raw";
        consumer.subscribe(Collections.singletonList(topic));

        Map<String, PlacarStore> placaresAtivos = new HashMap<>();
        String idTeamA = "FIFATMA";

        try {
            while (true) {
                ConsumerRecords<String, EventoFutebol> records = consumer.poll(Duration.ofMillis(100));

                StreamSupport.stream(records.spliterator(), false)
                        .filter(record -> record.value() != null && record.value().getType() != null)
                        .filter(record -> {
                            EventoFutebol evento = record.value();
                            boolean isShot = "SHOT".equals(evento.getType().getName());
                            boolean hasSubtypes = evento.getSubtypes() != null;

                            return isShot && hasSubtypes && evento.getSubtypes().stream()
                                    .anyMatch(subtype -> "GOAL".equals(subtype.getName()));
                        })
                        .forEach(record -> {
                            EventoFutebol eventoFutebol = record.value();
                            String matchId = record.key() != null ? record.key() : "Game_Unknown";

                            PlacarStore placar = placaresAtivos.computeIfAbsent(matchId,
                                    k -> new PlacarStore("Team A", "Team B"));

                            String scorerTeam = eventoFutebol.getTeam() != null ? eventoFutebol.getTeam().getId() : "Unknown";

                            System.out.print("GOOOOOOOOOOOOOOOOL! [" + matchId + " | Tempo: " + eventoFutebol.retornaTempoRegulamentar() + "] -- ");
                            placar.registrarGol(scorerTeam, idTeamA);
                        });
            }
        } catch (Exception e) {
            System.err.println("ERRO (ConsumerPlacar): " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}