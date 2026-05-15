package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MonitoramentoPartida {

    static class EstadoTatico {
        int acoesSequenciais = 0;
        long janelaTempoInicio = System.currentTimeMillis();
        String timeEmPosse = "";
    }

    public static void main(String[] args) {
        KafkaConsumer<String, EventoFutebol> consumer = KafkaCreate.createConsumer("monitoramento-group");
        consumer.subscribe(Collections.singletonList("match-events-raw"));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventoTaticoSerializer.class.getName());
        KafkaProducer<String, EventoTatico> producer = new KafkaProducer<>(producerProps);

        Map<String, EstadoTatico> estadoPorPartida = new HashMap<>();

        try {
            while (true) {
                ConsumerRecords<String, EventoFutebol> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, EventoFutebol> record : records) {
                    EventoFutebol evento = record.value();

                    if (evento == null || evento.getType() == null) continue;

                    String tipo = evento.getType().getName();
                    if (!("PASS".equals(tipo) || "SHOT".equals(tipo) || "CARRY".equals(tipo))) continue;

                    Location loc = evento.getStart();
                    if (loc == null || loc.getX() == null) continue;

                    String matchId = record.key() != null ? record.key() : "Game_Unknown";
                    String time = (evento.getTeam() != null) ? evento.getTeam().getName() : "Unknown";

                    estadoPorPartida.putIfAbsent(matchId, new EstadoTatico());
                    EstadoTatico estadoAtual = estadoPorPartida.get(matchId);

                    double x = loc.getX();
                    int periodo = evento.getPeriod();
                    boolean isMandante = "Team A".equals(time);
                    boolean isZonaDeAtaque = false;

                    if (isMandante) {
                        if (periodo == 1 && x >= 0.67) isZonaDeAtaque = true;
                        else if (periodo == 2 && x <= 0.33) isZonaDeAtaque = true;
                    } else {
                        if (periodo == 1 && x <= 0.33) isZonaDeAtaque = true;
                        else if (periodo == 2 && x >= 0.67) isZonaDeAtaque = true;
                    }

                    if (isZonaDeAtaque) {
                        long tempoAtual = System.currentTimeMillis();

                        if (!time.equals(estadoAtual.timeEmPosse) || (tempoAtual - estadoAtual.janelaTempoInicio) > 10000) {
                            estadoAtual.acoesSequenciais = 1;
                            estadoAtual.timeEmPosse = time;
                            estadoAtual.janelaTempoInicio = tempoAtual;
                        } else {
                            estadoAtual.acoesSequenciais++;
                        }
                        if (estadoAtual.acoesSequenciais >= 10) {
                            String tempo = evento.retornaTempoRegulamentar();

                            EventoTatico eventoTatico = new EventoTatico(matchId, time, "Pressão ofensiva alta", tempo);

                            producer.send(new ProducerRecord<>("match-insight", matchId, eventoTatico));
                            estadoAtual.acoesSequenciais = 0;
                        }
                    } else {
                        estadoAtual.acoesSequenciais = 0;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            producer.close();
        }
    }
}