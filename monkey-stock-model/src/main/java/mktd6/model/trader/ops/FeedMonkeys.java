package mktd6.model.trader.ops;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class FeedMonkeys extends TraderOp {

    private final int monkeys;

    @JsonCreator
    public FeedMonkeys(@JsonProperty("time") DateTime time,
                       @JsonProperty("txnId") String txnId,
                       @JsonProperty("monkeys") int monkeys) {
        super(time, txnId);
        if (monkeys < 1) {
            throw new IllegalArgumentException("Monkey no happy.");
        }
        this.monkeys = monkeys;
    }

    public static FeedMonkeys make(String txnId, int monkeys) {
        return new FeedMonkeys(DateTime.now(DateTimeZone.UTC), txnId, monkeys);
    }

    public int getMonkeys() {
        return monkeys;
    }

}
