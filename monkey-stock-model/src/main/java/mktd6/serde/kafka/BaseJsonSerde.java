package mktd6.serde.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class BaseJsonSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T> {

    private final Class<T> type;
    private final ObjectMapper mapper = new ObjectMapper();
    {
        this.mapper
            .registerModule(new JodaModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public BaseJsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0) { return null; }
        try {
            return mapper.readValue(bytes, type);
        }
        catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(String s, T t) {
        if (t == null) { return null; }
        try {
            return mapper.writeValueAsBytes(t);
        }
        catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    public String toJson(T obj) {
        try {
            return this.mapper.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            return "oups";
        }
    }

    @Override
    public void close() {}

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    public Class<T> getType() {
        return type;
    }
}
