package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static java.util.List.*;

public class Consumer {
    public static void main(String[] args) throws Exception {
        KafkaConsumer<String, EventoFutebol> consumer = getStringEventoFutebolKafkaConsumer();

        String topic = "match-events-raw";
        consumer.subscribe(of(topic));

        List<EventoFutebol> lista = new ArrayList<>();

        try {
            while (true) {
                ConsumerRecords<String, EventoFutebol> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, EventoFutebol> record : records) {
                    EventoFutebol eventoFutebol = record.value();
                    lista.add(eventoFutebol);

                    eventoFutebol.imprimeEvento();

//                    System.out.println("Received: " + eventoFutebol +
//                            " | partition= " + record.partition() +
//                            " | offset= " + record.offset());
                }

            }
        } finally {
            consumer.close();
        }
    }

    private static KafkaConsumer<String, EventoFutebol> getStringEventoFutebolKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "match-group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventoFutebolDeserializer.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }
}
