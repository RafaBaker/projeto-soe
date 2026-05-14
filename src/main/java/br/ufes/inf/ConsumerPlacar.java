package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

import static java.util.List.*;

public class ConsumerPlacar {
    public static void main(String[] args) throws Exception {
        KafkaConsumer<String, EventoFutebol> consumer = KafkaCreate.createConsumer("placar-group");

        String topic = "match-events-raw";
        consumer.subscribe(Collections.singletonList(topic));

        Map<String, PlacarStore> placaresAtivos = new HashMap<>();
        String idTeamA = "FIFATMA";

        try {
            while (true) {
                ConsumerRecords<String, EventoFutebol> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, EventoFutebol> record : records) {
                    EventoFutebol eventoFutebol = record.value();

                    if (eventoFutebol == null || eventoFutebol.getType() == null) continue;

                    String matchId = record.key() != null ? record.key(): "Game_Unknown";

                    placaresAtivos.putIfAbsent(matchId, new PlacarStore("Team A", "Team B"));

                    PlacarStore placar = placaresAtivos.get(matchId);

                    boolean isGol = false;

                    if ("SHOT".equals(eventoFutebol.getType().getName()) && eventoFutebol.getSubtypes() != null) {
                        for (Item subtype : eventoFutebol.getSubtypes()) {
                            if ("GOAL".equals(subtype.getName())) {
                                isGol = true;
                                break;
                            }
                        }
                    }

                    if (isGol) {
                        String scorerTeam = eventoFutebol.getTeam() != null ? eventoFutebol.getTeam().getId() : "Unknown";
                        System.out.print("GOOOOOOOOOOOOOOOOL! [" + matchId + "] -- ");

                        placar.registrarGol(scorerTeam, idTeamA);
                    }
                }

            }
        } catch (Exception e) {
            System.err.println("ERRO (ConsumerPlacar): " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
