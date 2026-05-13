package br.ufes.inf;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.util.List;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Producer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventoFutebolSerializer.class.getName());

        KafkaProducer<String, EventoFutebol> producer = new KafkaProducer<>(props);
        String topic = "match-events-raw";

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

        try {
            System.out.println("Lendo o json...");

            JsonNode rootNode = mapper.readTree(new File("Sample_Game_3_events.json"));

            JsonNode dataNode = rootNode.get("data");

            List<EventoFutebol> eventos = mapper.convertValue(dataNode, new TypeReference<List<EventoFutebol>>() {});

//            int i = 0;
            for (EventoFutebol evento : eventos) {

                System.out.println("Criando evento");
                ProducerRecord<String, EventoFutebol> record = new ProducerRecord<>(topic, evento.getFrom().getId(), evento);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Enviando EventoFutebol..." +  evento +
                                            "| partition=" + metadata.partition() +
                                            "| offset=" + metadata.offset());
                    } else {
                        System.out.println("Algum erro aconteceu" + exception.getMessage());;
                    }
                });

                Thread.sleep(100);
            }
//            producer.flush();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        finally {
            producer.close();
            System.out.println("produtor encerrado");
        }
    }
}