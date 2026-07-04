package br.ufes.inf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerPlacar {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "placar-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Serde<EventoFutebol> eventoSerde = Serdes.serdeFrom(new EventoFutebolSerializer(), new EventoFutebolDeserializer());

        // Criando um Serde customizado simples usando Jackson para o PlacarStore
        ObjectMapper mapper = new ObjectMapper();
        Serde<PlacarStore> placarSerde = Serdes.serdeFrom(
            (topic, data) -> {
                try { return data == null ? null : mapper.writeValueAsBytes(data); } 
                catch (JsonProcessingException e) { throw new RuntimeException(e); }
            },
            (topic, data) -> {
                try { return data == null ? null : mapper.readValue(data, PlacarStore.class); } 
                catch (Exception e) { throw new RuntimeException(e); }
            }
        );

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, EventoFutebol> stream = builder.stream("match-events-raw", 
                Consumed.with(Serdes.String(), eventoSerde));

        final String idTeamA = "FIFATMA";

        stream.filter((key, record) -> record != null && record.getType() != null)
              .filter((key, record) -> {
                  EventoFutebol evento = record;
                  boolean isShot = "SHOT".equals(evento.getType().getName());
                  boolean hasSubtypes = evento.getSubtypes() != null;
                  return isShot && hasSubtypes && evento.getSubtypes().stream()
                          .anyMatch(subtype -> "GOAL".equals(subtype.getName()));
              })
              // Definindo explicitamente a chave como sendo o matchId, em caso de eventos soltos
              .selectKey((key, record) -> key != null ? key : "Game_Unknown")
              // Agrupando pelo matchId para gerenciar o estado particionado
              .groupByKey(Grouped.with(Serdes.String(), eventoSerde))
              // Substituindo o nó Processor por um nó de agregação da DSL
              .aggregate(
                  () -> new PlacarStore("Team A", "Team B"), // Inicializador do Store
                  (matchId, eventoFutebol, placar) -> {      // Função agregadora de Estado (Reducer)
                      String scorerTeam = eventoFutebol.getTeam() != null ? eventoFutebol.getTeam().getId() : "Unknown";
                      System.out.print("GOOOOOOOOOOOOOOOOL! [" + matchId + " | Tempo: " + eventoFutebol.retornaTempoRegulamentar() + "] -- ");
                      placar.registrarGol(scorerTeam, idTeamA);
                      return placar;
                  },
                  Materialized.with(Serdes.String(), placarSerde) // Armazenando e tipando o state store localmente
              );

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }
    }
}