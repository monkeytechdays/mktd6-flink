package mktd6.flink;

import mktd6.model.trader.Trader;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class TraderKeySelector<T> implements KeySelector<Tuple2<Trader, T>, String> {
    @Override
    public String getKey(Tuple2<Trader, T> value) throws Exception {
        return value.f0.getTeam().name() + "_" + value.f0.getName();
    }
}
