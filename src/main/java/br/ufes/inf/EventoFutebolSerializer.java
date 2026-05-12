package br.ufes.inf;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EventoFutebolSerializer implements Serializer<EventoFutebol> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, EventoFutebol eventoFutebol) {
        try {
            if (eventoFutebol == null) {
                return null;
            }
            return objectMapper.writeValueAsBytes(eventoFutebol);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao serializar EventoFutebol", e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, EventoFutebol data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
