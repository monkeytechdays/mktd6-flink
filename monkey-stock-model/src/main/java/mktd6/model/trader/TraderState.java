package mktd6.model.trader;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import mktd6.model.Timestamped;
import mktd6.model.market.ops.TxnResultType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class TraderState implements Timestamped {

    private final DateTime time;

    private final double coins;
    private final int shares;
    private final int bailouts;
    private final int fedMonkeys;
    private final int inFlightInvestments;

    @JsonCreator
    public TraderState(
            @JsonProperty("time") DateTime time,
            @JsonProperty("coins") double coins,
            @JsonProperty("shares") int shares,
            @JsonProperty("bailouts") int bailouts,
            @JsonProperty("fedMonkeys") int fedMonkeys,
            @JsonProperty("inFlightInvestments") int inFlightInvestments) {
        this.time = time;
        this.coins = coins;
        this.shares = shares;
        this.bailouts = bailouts;
        this.fedMonkeys = fedMonkeys;
        this.inFlightInvestments = inFlightInvestments;
    }

    public TraderState(double coins, int shares, int bailouts, int fedMonkeys, int inFlightInvestments) {
        this.time = now();
        this.coins = coins;
        this.shares = shares;
        this.bailouts = bailouts;
        this.fedMonkeys = fedMonkeys;
        this.inFlightInvestments = inFlightInvestments;
    }

    public DateTime getTime() {
        return time;
    }

    public double getCoins() {
        return coins;
    }

    public int getShares() {
        return shares;
    }

    public int getBailouts() {
        return bailouts;
    }

    public int getFedMonkeys() {
        return fedMonkeys;
    }

    public int getInFlightInvestments() {
        return inFlightInvestments;
    }

    public TxnResultType validate() {
        return
            (coins < 0) ? TxnResultType.INSUFFICIENT_COINS :
            (shares < 0) ? TxnResultType.INSUFFICIENT_SHARES :
            TxnResultType.ACCEPTED;
    }

    public static TraderState init() {
        return new TraderState(
            10,
            5,
            0,
            0,
            0);
    }

    private static DateTime now() {
        return DateTime.now(DateTimeZone.UTC);
    }

}

