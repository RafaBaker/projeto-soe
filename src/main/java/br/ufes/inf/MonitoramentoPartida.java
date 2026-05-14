package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Classe responsável pelo monitoramento em tempo real dos eventos da partida.
 * Implementa 3 situações de interesse, 2 com eventos primitivos e 1 com evento derivado.
 */
public class MonitoramentoPartida {

    public static void main(String[] args) {

        Properties consumerProps = new Properties();
        // Conexão com os 3 brokers com Docker
        consumerProps.put("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092");
        consumerProps.put("group.id", "grupo-monitoramento-estatistico");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "br.ufes.inf.EventoFutebolDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, EventoFutebol> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("match-events-raw"));

        // Configuração do Producer
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        int acoesSequenciais = 0;
        long janelaTempoInicio = System.currentTimeMillis();
        String timeEmPosse = "";
        int ladoPressao = 0;

        System.out.println("--- MONITOR DE EVENTOS INICIADO ---");

        try {
            while (true) {
                ConsumerRecords<String, EventoFutebol> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, EventoFutebol> record : records) {
                    EventoFutebol evento = record.value();
                    if (evento == null || evento.getType() == null) continue;

                    String tipo = evento.getType().getName();
                    String time = (evento.getTeam() != null) ? evento.getTeam().getName() : "Desconhecido";

                    // Situação de interesse 1: Falta Perigosa
                    if ("FAULT RECEIVED".equals(tipo)) {
                        Location loc = evento.getStart();
                        if (loc != null && loc.getX() != null) {
                            // Áreas ficam abaixo de 0.25 ou acima de 0.75
                            if (loc.getX() <= 0.25 || loc.getX() >= 0.75) {
                                System.out.printf("[ALERTA] Falta Perigosa para o %s em X: %.2f%n", time, loc.getX());
                            }
                        }
                    }

                    // Situação de interesse 2: Finalização
                    if ("SHOT".equals(tipo)) {
                        System.out.println("[ALERTA] Finalização detectada! Risco de gol para o " + time);
                    }

                    // Situação de interesse 3: Ação disciplinar (CARD)
                    if ("CARD".equals(tipo)) {
                        System.out.println("[AVISO] Cartão aplicado na partida!");
                    }

                    // Situação de interesse complexa ---
                    // Se o mesmo time realiza 8 ações (passes, chutes ou conduções) no campo adversário
                    // num intervalo de processamento curto, inferimos como "Pressão ofensiva".
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

                                if (!time.equals(timeEmPosse) || (tempoAtual - janelaTempoInicio) > 10000) {
                                    acoesSequenciais = 1;
                                    timeEmPosse = time;
                                    janelaTempoInicio = tempoAtual;
                                } else {
                                    acoesSequenciais++;
                                }

                                if (acoesSequenciais >= 8) {
                                    System.out.println("--- INFERÊNCIA: Pressão ofensiva do time " + time + " no campo de ataque! ---");

                                    String jsonDerivado = String.format(
                                            "{\"insight\": \"High Offensive Pressure\", \"team\": \"%s\", \"period\": %d, \"timestamp\": %d}",
                                            time, periodo, tempoAtual
                                    );

                                    producer.send(new ProducerRecord<>("match-insight", time, jsonDerivado));

                                    acoesSequenciais = 0;
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