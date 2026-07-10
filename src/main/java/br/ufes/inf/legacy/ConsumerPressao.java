package br.ufes.inf.legacy;

import br.ufes.inf.*;

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

public class ConsumerPressao {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pression-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Serde<EventoTatico> taticoSerde = Serdes.serdeFrom(new EventoTaticoSerializer(), new EventoTaticoDeserializer());

        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, EventoTatico> stream = builder.stream("match-insight", 
            Consumed.with(Serdes.String(), taticoSerde));

        stream.filter((key, alerta) -> alerta != null)
              .peek((key, alerta) -> {
                  System.out.printf("[INSIGHT | %s | Tempo: %s] %s detectada da equipe: %s!%n",
                                    alerta.getMatchId(),
                                    alerta.getTempoRegulamentar(),
                                    alerta.getInsight(),
                                    alerta.getTeam());
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