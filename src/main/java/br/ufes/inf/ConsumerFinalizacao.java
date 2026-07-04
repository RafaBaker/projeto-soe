package br.ufes.inf;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerFinalizacao {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "shots-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        Serde<EventoFutebol> eventoFutebolSerde = Serdes.serdeFrom(new EventoFutebolSerializer(), new EventoFutebolDeserializer());

        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, EventoFutebol> stream = builder.stream("match-events-raw", 
            Consumed.with(Serdes.String(), eventoFutebolSerde));

        stream.filter((key, evento) -> evento != null && evento.getType() != null)
              .filter((key, evento) -> "SHOT".equals(evento.getType().getName()))
              .peek((key, evento) -> {
                  String matchId = key != null ? key : "Unknown_Game";
                  String time = (evento.getTeam() != null) ? evento.getTeam().getName() : "Unknown";

                  System.out.printf("[ALERTA | %s | Tempo: %s] Finalização do time '%s'!%n",
                                    matchId, evento.retornaTempoRegulamentar(), time);
              });

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