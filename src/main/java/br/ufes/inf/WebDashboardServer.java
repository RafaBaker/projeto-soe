package br.ufes.inf;

import io.javalin.Javalin;
import io.javalin.websocket.WsContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class WebDashboardServer {

    private static final Set<WsContext> clients = ConcurrentHashMap.newKeySet();
    private static final Set<WsContext> heatmapClients = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> {
            config.staticFiles.add("/public");
        }).start(8080);

        app.ws("/insights", ws -> {
            ws.onConnect(ctx -> {
                System.out.println("Cliente conectado: " + ctx.sessionId());
                // Configurar timeout do WebSocket
                ctx.session.setIdleTimeout(Duration.ofMinutes(15));
                clients.add(ctx);
            });
            ws.onMessage(ctx -> {
                // Responder ping para manter sessão ativa
                if ("ping".equals(ctx.message())) {
                    ctx.send("pong");
                }
            });
            ws.onClose(ctx -> {
                System.out.println("Cliente desconectado: " + ctx.sessionId());
                clients.remove(ctx);
            });
            ws.onError(ctx -> System.out.println("Erro no websocket"));
        });

        app.ws("/heatmap-ws", ws -> {
            ws.onConnect(ctx -> heatmapClients.add(ctx));
            ws.onClose(ctx -> heatmapClients.remove(ctx));
        });

        startKafkaConsumer();
        startHeatmapConsumer();
    }

    private static void startKafkaConsumer() {
        new Thread(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "web-dashboard-group-" + System.currentTimeMillis());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventoTaticoDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); 

            KafkaConsumer<String, EventoTatico> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList("match-insight"));

            try {
                while (true) {
                    ConsumerRecords<String, EventoTatico> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, EventoTatico> record : records) {
                        EventoTatico insight = record.value();
                        if (insight == null) continue; // Ignora eventos tombstone/vazios

                        String jsonAlert = String.format("{\"matchId\":\"%s\", \"team\":\"%s\", \"insight\":\"%s\", \"time\":\"%s\"}",
                                insight.getMatchId(), insight.getTeam(), insight.getInsight(), insight.getTempoRegulamentar());

                        for (WsContext ctx : clients) {
                            if (ctx.session.isOpen()) {
                                ctx.send(jsonAlert);
                            }
                        }
                        System.out.println("Broadcasted: " + jsonAlert);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }).start();
    }

    private static void startHeatmapConsumer() {
        new Thread(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "web-heatmap-group-" + System.currentTimeMillis());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); 

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList("match-heatmap"));

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        String jsonStats = record.value();
                        if (jsonStats == null) continue;
                        
                        for (WsContext ctx : heatmapClients) {
                            if (ctx.session.isOpen()) {
                                ctx.send(jsonStats);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }).start();
    }
}
