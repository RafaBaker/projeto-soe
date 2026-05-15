package br.ufes.inf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class EventoTaticoDeserializer implements Deserializer<EventoTatico> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public EventoTatico deserialize(String topic, byte[] data) {
        try {
            if (data == null) return null;
            return objectMapper.readValue(data, EventoTatico.class);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao deserializar AlertaTatico", e);
        }
    }
}