package mktd6;

import mktd6.model.Timestamped;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TimestampExtractor<T extends Timestamped> extends BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, T>> {

    public TimestampExtractor() {
        super(Time.milliseconds(10));
    }

    @Override
    public long extractTimestamp(Tuple2<String, T> element) {
        return element.f1.getTime().getMillis();
    }
}