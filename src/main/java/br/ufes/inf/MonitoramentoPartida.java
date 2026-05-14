package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Classe responsável pelo monitoramento em tempo real dos eventos da partida.
 */
public class MonitoramentoPartida {

    static class EstadoTatico {
        int acoesSequenciais = 0;
        long janelaTempoInicio = System.currentTimeMillis();
        String timeEmPosse = "";
    }

    public static void main(String[] args) {

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092");
        consumerProps.put("group.id", "grupo-monitoramento-estatistico");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "br.ufes.inf.EventoFutebolDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, EventoFutebol> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("match-events-raw"));

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        Map<String, EstadoTatico> estadoPorPartida = new HashMap<>();

        System.out.println("--- MONITOR DE EVENTOS INICIADO ---");

        try {
            while (true) {
                ConsumerRecords<String, EventoFutebol> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, EventoFutebol> record : records) {
                    EventoFutebol evento = record.value();
                    if (evento == null || evento.getType() == null) continue;

                    String matchId = record.key() != null ? record.key() : "Partida_Desconhecida";

                    estadoPorPartida.putIfAbsent(matchId, new EstadoTatico());

                    EstadoTatico estadoAtual = estadoPorPartida.get(matchId);

                    String tipo = evento.getType().getName();
                    String time = (evento.getTeam() != null) ? evento.getTeam().getName() : "Desconhecido";

                    if ("FAULT RECEIVED".equals(tipo)) {
                        Location loc = evento.getStart();
                        if (loc != null && loc.getX() != null) {
                            if (loc.getX() <= 0.25 || loc.getX() >= 0.75) {
                                System.out.printf("[ALERTA | %s] Falta Perigosa para '%s' em X: %.2f%n", matchId, time, loc.getX());
                            }
                        }
                    }

                    if ("SHOT".equals(tipo)) {
                        System.out.printf("[ALERTA | %s] Finalização detectada! Risco de gol para '%s'%n", matchId, time);
                    }

                    if ("CARD".equals(tipo)) {
                        System.out.printf("[AVISO | %s] Cartão aplicado na partida!%n", matchId);
                    }

                    if ("PASS".equals(tipo) || "SHOT".equals(tipo) || "CARRY".equals(tipo)) {
                        Location loc = evento.getStart();

                        if (loc != null && loc.getX() != null) {
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

                                if (estadoAtual.acoesSequenciais >= 8) {
                                    System.out.println("--- INFERÊNCIA [" + matchId + "]: Pressão de '" + time + "' no campo de ataque! ---");

                                    String jsonDerivado = String.format(
                                            "{\"match_id\": \"%s\", \"insight\": \"High Offensive Pressure\", \"team\": \"%s\", \"period\": %d, \"timestamp\": %d}",
                                            matchId, time, periodo, tempoAtual
                                    );

                                    producer.send(new ProducerRecord<>("match-insight", matchId, jsonDerivado));

                                    estadoAtual.acoesSequenciais = 0;
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }
}