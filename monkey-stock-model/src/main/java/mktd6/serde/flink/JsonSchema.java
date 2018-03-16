package mktd6.serde.flink;

import mktd6.serde.kafka.BaseJsonSerde;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.io.IOException;

public class JsonSchema<K, V> implements KeyedDeserializationSchema<Tuple2<K, V>>, KeyedSerializationSchema<Tuple2<K, V>> {

    private final Class<K> keyType;
    private final Class<V> valueType;
    private transient BaseJsonSerde<K> keySerializer;
    private transient BaseJsonSerde<V> valueSerializer;

    public JsonSchema(Class<K> keyType, Class<V> valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public TypeInformation<Tuple2<K, V>> getProducedType() {
        return new TupleTypeInfo<>(
                TypeInformation.of(keyType),
                TypeInformation.of(valueType)
        );
    }

    @Override
    public Tuple2<K, V> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        if (keySerializer == null) {
            keySerializer = new BaseJsonSerde<>(keyType);
        }
        if (valueSerializer == null) {
            valueSerializer = new BaseJsonSerde<V>(valueType);
        }

        return Tuple2.of(
                keySerializer.deserialize(null, messageKey),
                valueSerializer.deserialize(null, message)
        );
    }

    @Override
    public boolean isEndOfStream(Tuple2<K, V> nextElement) {
        return false;
    }

    @Override
    public byte[] serializeKey(Tuple2<K, V> element) {
        if (keySerializer == null) {
            keySerializer = new BaseJsonSerde<>(keyType);
        }
        return keySerializer.serialize(null, element.f0);
    }

    @Override
    public byte[] serializeValue(Tuple2<K, V> element) {
        if (valueSerializer == null) {
            valueSerializer = new BaseJsonSerde<>(valueType);
        }
        return valueSerializer.serialize(null, element.f1);
    }

    @Override
    public String getTargetTopic(Tuple2<K, V> element) {
        return null;
    }
}
