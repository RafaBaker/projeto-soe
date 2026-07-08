package br.ufes.inf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MonitoramentoPartida {

    // Classe para armazenar o estado da agregação da janela
    public static class AgregadorPressao {
        public int contagem = 0;
        public String ultimoTempo = "00:00";

        public AgregadorPressao() {}
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "monitoramento-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Serde<EventoFutebol> eventoSerde = Serdes.serdeFrom(new EventoFutebolSerializer(), new EventoFutebolDeserializer());
        Serde<EventoTatico> taticoSerde = Serdes.serdeFrom(new EventoTaticoSerializer(), new EventoTaticoDeserializer());

        // Criando um Serde customizado simples usando Jackson para o nosso agregador
        ObjectMapper mapper = new ObjectMapper();
        Serde<AgregadorPressao> agregadorSerde = Serdes.serdeFrom(
            (topic, data) -> {
                try { return data == null ? null : mapper.writeValueAsBytes(data); } 
                catch (JsonProcessingException e) { throw new RuntimeException(e); }
            },
            (topic, data) -> {
                try { return data == null ? null : mapper.readValue(data, AgregadorPressao.class); } 
                catch (Exception e) { throw new RuntimeException(e); }
            }
        );

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, EventoFutebol> sourceStream = builder.stream("match-events-raw", 
                Consumed.with(Serdes.String(), eventoSerde));

        sourceStream
                .filter((key, record) -> record != null && record.getType() != null)
                .filter((key, record) -> {
                    String tipo = record.getType().getName();
                    return "PASS".equals(tipo) || "SHOT".equals(tipo) || "CARRY".equals(tipo);
                })
                .filter((key, record) -> {
                    if (record.getStart() == null || record.getStart().getX() == null) return false;
                    String time = (record.getTeam() != null) ? record.getTeam().getName() : "Unknown";
                    
                    double x = record.getStart().getX();
                    int periodo = record.getPeriod();
                    boolean isMandante = "Team A".equals(time);

                    if (isMandante) {
                        return (periodo == 1 && x >= 0.67) || (periodo == 2 && x <= 0.33);
                    } else {
                        return (periodo == 1 && x <= 0.33) || (periodo == 2 && x >= 0.67);
                    }
                })
                // Usando a chave composta: MatchId @ Time para agrupar as ações daquele time no jogo
                .selectKey((key, record) -> {
                    String matchId = key != null ? key : "Game_Unknown";
                    String time = (record.getTeam() != null) ? record.getTeam().getName() : "Unknown";
                    return matchId + "@" + time;
                })
                .groupByKey(Grouped.with(Serdes.String(), eventoSerde))
                
                // Sliding Window
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(2)))
                
                // Agreggate com window
                .aggregate(
                        () -> new AgregadorPressao(), // Inicializador
                        (aggKey, newValue, aggValue) -> { // Agregador
                            aggValue.contagem += 1;
                            aggValue.ultimoTempo = newValue.retornaTempoRegulamentar();
                            return aggValue;
                        },
                        Materialized.with(Serdes.String(), agregadorSerde)
                )
                .toStream() // Converte de KTable de volta para KStream
                
                // Filtra as janelas onde houveram 10 ou mais ações sequenciais de pressão
                .filter((windowedKey, agregador) -> agregador != null && agregador.contagem >= 10)
                
                // Mapeia para o EventoTatico final e restaura a chave original (matchId)
                .map((windowedKey, agregador) -> {
                    String[] parts = windowedKey.key().split("@");
                    String matchId = parts[0];
                    String time = parts.length > 1 ? parts[1] : "Unknown";
                    
                    EventoTatico eventoTatico = new EventoTatico(matchId, time, "Pressão ofensiva alta", agregador.ultimoTempo);
                    return new KeyValue<>(matchId, eventoTatico);
                })
                
                .peek((windowedKey, eventoTatico) -> {
                    System.out.printf("[INSIGHT GERADO | %s | Tempo: %s] %s detectada da equipe: %s!%n",
                            eventoTatico.getMatchId(),
                            eventoTatico.getTempoRegulamentar(),
                            eventoTatico.getInsight(),
                            eventoTatico.getTeam());
                })
                .to("match-insight", Produced.with(Serdes.String(), taticoSerde));

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