package br.ufes.inf;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class EventoFutebolTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof EventoFutebol) {
            EventoFutebol evento = (EventoFutebol) record.value();
            if (evento != null && evento.getStart() != null && evento.getStart().getTime() != null) {
                // time comes in seconds from the JSON. Convert to milliseconds for Kafka
                return (long) (evento.getStart().getTime() * 1000);
            }
        }
        // Fallback to the partition time if we can't extract it
        return partitionTime;
    }
}
