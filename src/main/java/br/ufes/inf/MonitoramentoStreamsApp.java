package br.ufes.inf;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MonitoramentoStreamsApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "monitoramento-cep-app-" + System.currentTimeMillis()); 
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        props.put(StreamsConfig.consumerPrefix(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventoFutebolTimestampExtractor.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Desativa o cache para forçar envio imediato

        StreamsBuilder builder = new StreamsBuilder();

        Serde<EventoFutebol> eventoFutebolSerde = Serdes.serdeFrom(new EventoFutebolSerializer(), new EventoFutebolDeserializer());
        Serde<EventoTatico> eventoTaticoSerde = Serdes.serdeFrom(new EventoTaticoSerializer(), new EventoTaticoDeserializer());

        KStream<String, EventoFutebol> rawStream = builder.stream("match-events-raw", Consumed.with(Serdes.String(), eventoFutebolSerde))
                .filter((k, v) -> v != null && v.getType() != null && v.getTeam() != null)
                .peek((k, v) -> System.out.println("Lido do Kafka: " + v.getType().getName()));

        KStream<String, EventoFutebol> streamByTeam = rawStream.selectKey((k, v) -> k + "_" + v.getTeam().getName());


        // ==========================================
        // NOVOS: EVENTOS SIMPLES (STATELESS)
        // ==========================================

        // --- GOL (PLACAR) ---
        rawStream
                .filter((k, v) -> "SHOT".equals(v.getType().getName()) && v.getSubtypes() != null && v.getSubtypes().stream().anyMatch(s -> "GOAL".equals(s.getName())))
                .map((k, v) -> {
                    String matchId = k != null ? k : "Game_3";
                    EventoTatico insight = new EventoTatico(matchId, v.getTeam().getName(), "GOL! ⚽🥅", v.retornaTempoRegulamentar());
                    return new KeyValue<>(matchId, insight);
                })
                .to("match-insight", Produced.with(Serdes.String(), eventoTaticoSerde));

        // --- CARTÃO ---
        rawStream
                .filter((k, v) -> "CARD".equals(v.getType().getName()))
                .map((k, v) -> {
                    String matchId = k != null ? k : "Game_3";
                    EventoTatico insight = new EventoTatico(matchId, v.getTeam().getName(), "Cartão Aplicado 🟨/🟥", v.retornaTempoRegulamentar());
                    return new KeyValue<>(matchId, insight);
                })
                .to("match-insight", Produced.with(Serdes.String(), eventoTaticoSerde));

        // --- FINALIZAÇÃO (NÃO GOL) ---
        rawStream
                .filter((k, v) -> "SHOT".equals(v.getType().getName()) && (v.getSubtypes() == null || v.getSubtypes().stream().noneMatch(s -> "GOAL".equals(s.getName()))))
                .map((k, v) -> {
                    String matchId = k != null ? k : "Game_3";
                    EventoTatico insight = new EventoTatico(matchId, v.getTeam().getName(), "Finalização 🎯", v.retornaTempoRegulamentar());
                    return new KeyValue<>(matchId, insight);
                })
                .to("match-insight", Produced.with(Serdes.String(), eventoTaticoSerde));


        // ==========================================
        // EVENTOS COMPLEXOS CEP (STATEFUL)
        // ==========================================

        // --- TOPOLOGIA 1: PRESSÃO OFENSIVA ALTA ---
        streamByTeam
                .filter((k, v) -> "PASS".equals(v.getType().getName()) || "SHOT".equals(v.getType().getName()) || "CARRY".equals(v.getType().getName()))
                .filter((k, v) -> {
                    if (v.getStart() == null || v.getStart().getX() == null) return false;
                    double x = v.getStart().getX();
                    int periodo = v.getPeriod();
                    boolean isMandante = "Team A".equals(v.getTeam().getName());
                    if (isMandante) {
                        return (periodo == 1 && x >= 0.67) || (periodo == 2 && x <= 0.33);
                    } else {
                        return (periodo == 1 && x <= 0.33) || (periodo == 2 && x >= 0.67);
                    }
                })
                .groupByKey(Grouped.with(Serdes.String(), eventoFutebolSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(45))) // 45s de janela
                .count()
                .toStream()
                .filter((windowedKey, count) -> count != null && count == 12) // Emite EXATAMENTE na 12ª ação para não flodar
                .map((windowedKey, count) -> {
                    String[] parts = windowedKey.key().split("_");
                    String matchId = parts[0] + "_" + parts[1];
                    String team = parts.length > 2 ? parts[2] : "Unknown";
                    EventoTatico insight = new EventoTatico(matchId, team, "Pressão Ofensiva Alta (🔥)", "[Insight]");
                    return new KeyValue<>(matchId, insight);
                })
                .to("match-insight", Produced.with(Serdes.String(), eventoTaticoSerde));


        // --- TOPOLOGIA 2: TIKI-TAKA / PASSAGEM RÁPIDA (DURING) ---
        streamByTeam
                .filter((k, v) -> "PASS".equals(v.getType().getName()))
                .groupByKey(Grouped.with(Serdes.String(), eventoFutebolSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(45))) // 45s de janela
                .count()
                .toStream()
                .filter((windowedKey, count) -> count != null && count == 10) // Emite EXATAMENTE no 10º passe (Tiki-Taka Real)
                .map((windowedKey, count) -> {
                    String[] parts = windowedKey.key().split("_");
                    String matchId = parts[0] + "_" + parts[1];
                    String team = parts.length > 2 ? parts[2] : "Unknown";
                    EventoTatico insight = new EventoTatico(matchId, team, "Tiki-Taka ⚽ (" + count + " passes)", "[Insight]");
                    return new KeyValue<>(matchId, insight);
                })
                .to("match-insight", Produced.with(Serdes.String(), eventoTaticoSerde));


        // --- TOPOLOGIA 3: DOMÍNIO TERRITORIAL (DURING - Session Windows) ---
        streamByTeam
                .filter((k, v) -> "PASS".equals(v.getType().getName()) || "CARRY".equals(v.getType().getName()))
                .groupByKey(Grouped.with(Serdes.String(), eventoFutebolSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30))) // Inactivity gap de 30s
                .count()
                .toStream()
                .filter((windowedKey, count) -> count != null && count == 25) // Emite EXATAMENTE na 25ª ação contínua
                .map((windowedKey, count) -> {
                    String[] parts = windowedKey.key().split("_");
                    String matchId = parts[0] + "_" + parts[1];
                    String team = parts.length > 2 ? parts[2] : "Unknown";
                    EventoTatico insight = new EventoTatico(matchId, team, "Domínio Territorial 🛡️ (" + count + " ações)", "[Insight]");
                    return new KeyValue<>(matchId, insight);
                })
                .to("match-insight", Produced.with(Serdes.String(), eventoTaticoSerde));


        // --- TOPOLOGIA 4: TUMULTO / AGRESSIVIDADE CRESCENTE (OVERLAPS) ---
        rawStream
                .filter((k, v) -> "FOUL".equals(v.getType().getName()) || "CARD".equals(v.getType().getName()))
                .groupByKey(Grouped.with(Serdes.String(), eventoFutebolSerde))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(10))) // 5 Minutos!
                .count()
                .toStream()
                .filter((windowedKey, count) -> count != null && count == 2) // Emite na 2ª infração apenas
                .map((windowedKey, count) -> {
                    EventoTatico insight = new EventoTatico(windowedKey.key(), "Ambos", "Jogo Faltoso / Tenso 🟥", "[Insight]");
                    return new KeyValue<>(windowedKey.key(), insight);
                })
                .to("match-insight", Produced.with(Serdes.String(), eventoTaticoSerde));


        // --- TOPOLOGIA 5: CONTRA-ATAQUE RÁPIDO (MEETS) ---
        KStream<String, EventoFutebol> defesas = streamByTeam
                .filter((k, v) -> "RECOVERY".equals(v.getType().getName()) || "INTERCEPTION".equals(v.getType().getName()) || "CLEARANCE".equals(v.getType().getName()));

        KStream<String, EventoFutebol> finalizacoes = streamByTeam
                .filter((k, v) -> "SHOT".equals(v.getType().getName()));

        ValueJoiner<EventoFutebol, EventoFutebol, EventoTatico> joiner = (defesa, chute) -> {
            return new EventoTatico("", 
                                    defesa.getTeam().getName(), 
                                    "Contra-Ataque Rápido Concluído ⚡", 
                                    "[Insight]");
        };

        defesas.join(finalizacoes,
                joiner,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(30), Duration.ZERO)
                    .after(Duration.ofSeconds(30))
                    .before(Duration.ZERO),
                StreamJoined.with(Serdes.String(), eventoFutebolSerde, eventoFutebolSerde))
                .map((k, v) -> {
                    String[] parts = k.split("_");
                    String matchId = parts[0] + "_" + parts[1];
                    v.setMatchId(matchId);
                    return new KeyValue<>(matchId, v);
                })
                .to("match-insight", Produced.with(Serdes.String(), eventoTaticoSerde));


        // ==========================================
        // EVENTOS EM CASCATA: PLAYER STATS & HEATMAP
        // ==========================================
        ObjectMapper mapper = new ObjectMapper();
        Serde<PlayerStats> playerStatsSerde = Serdes.serdeFrom(
            (topic, data) -> {
                try { return data == null ? null : mapper.writeValueAsBytes(data); } 
                catch (Exception e) { throw new RuntimeException(e); }
            },
            (topic, data) -> {
                try { return data == null ? null : mapper.readValue(data, PlayerStats.class); } 
                catch (Exception e) { throw new RuntimeException(e); }
            }
        );

        rawStream
            // Só interessa quem fez a ação e tem posição XYZ
            .filter((k, v) -> v.getFrom() != null && v.getStart() != null && v.getStart().getX() != null && v.getStart().getY() != null)
            // Agrupamos por jogador em vez de time!
            .selectKey((k, v) -> k + "_" + v.getFrom().getId())
            .groupByKey(Grouped.with(Serdes.String(), eventoFutebolSerde))
            .aggregate(
                () -> new PlayerStats(), // Inicializa as estatísticas zeradas
                (key, evento, stats) -> {
                    if (stats.getPlayerId() == null) {
                        String[] parts = key.split("_");
                        stats.setMatchId(parts[0] + "_" + parts[1]);
                        stats.setPlayerId(evento.getFrom().getId());
                        stats.setPlayerName(evento.getFrom().getName());
                        stats.setTeamId(evento.getTeam().getId());
                        stats.setTeamName(evento.getTeam().getName());
                    }
                    // Computa a ação na matriz 10x10 do Heatmap local do jogador
                    stats.addAction(evento.getStart().getX(), evento.getStart().getY());
                    return stats;
                },
                Materialized.with(Serdes.String(), playerStatsSerde)
            )
            .toStream() // Ramifica a KTable pronta em um KStream
            // Joga os dados pesados pro novo tópico, não misturando com o match-insight
            .to("match-heatmap", Produced.with(Serdes.String(), playerStatsSerde));

        Topology topology = builder.build();
        System.out.println("===============================================");
        System.out.println("DESCRIÇÃO DA TOPOLOGIA DO KAFKA STREAMS:");
        System.out.println(topology.describe());
        System.out.println("===============================================");

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler(e -> {
            System.err.println("CRITICAL STREAM ERROR:");
            e.printStackTrace();
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.cleanUp();
        streams.start();

        System.out.println("Kafka Streams CEP Engine iniciado...");
        
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
