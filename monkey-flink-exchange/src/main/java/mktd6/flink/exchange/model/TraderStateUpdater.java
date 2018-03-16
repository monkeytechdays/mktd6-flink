package mktd6.flink.exchange.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import mktd6.model.market.ops.TxnResult;
import mktd6.model.market.ops.TxnResultType;
import mktd6.model.trader.TraderState;
import mktd6.model.trader.ops.FeedMonkeys;
import mktd6.model.trader.ops.Investment;
import mktd6.model.trader.ops.MarketOrder;
import mktd6.serde.kafka.BaseJsonSerde;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public final class TraderStateUpdater {

    public static final TraderStateUpdater BAILOUT_UPDATER =
        new TraderStateUpdater("bailout", Type.BAILOUT, 10d, 5, true, 0, 0);

    public enum Type {
        MARKET,
        INVEST,
        FEED,
        BAILOUT,
        RETURN;
    }

    private final String txnId;
    private final Type type;
    private final DateTime time;

    private final double coinsDiff;
    private final int sharesDiff;
    private final boolean addBailout;
    private final int fedMonkeys;
    private final int investDiff;

    @JsonCreator
    public TraderStateUpdater(
            @JsonProperty("txnId") String txnId,
            @JsonProperty("type") Type type,
            @JsonProperty("time") DateTime time,
            @JsonProperty("coinsDiff") double coinsDiff,
            @JsonProperty("sharesDiff") int sharesDiff,
            @JsonProperty("addBailout") boolean addBailout,
            @JsonProperty("fedMonkeys") int fedMonkeys,
            @JsonProperty("investDiff") int investDiff
    ) {
        this.txnId = txnId;
        this.type = type;
        this.time = time;
        this.coinsDiff = coinsDiff;
        this.sharesDiff = sharesDiff;
        this.addBailout = addBailout;
        this.fedMonkeys = fedMonkeys;
        this.investDiff = investDiff;
    }

    public TraderStateUpdater(
            String txnId,
            Type type,
            double coinsDiff,
            int sharesDiff,
            boolean addBailout,
            int fedMonkeys,
            int investDiff
    ) {
        this.txnId = txnId;
        this.type = type;
        this.time = DateTime.now(DateTimeZone.UTC);
        this.coinsDiff = coinsDiff;
        this.sharesDiff = sharesDiff;
        this.addBailout = addBailout;
        this.fedMonkeys = fedMonkeys;
        this.investDiff = investDiff;
    }


    public DateTime getTime() {
        return time;
    }

    public double getCoinsDiff() {
        return coinsDiff;
    }

    public int getSharesDiff() {
        return sharesDiff;
    }

    public boolean getAddBailout() {
        return addBailout;
    }

    public int getFedMonkeys() {
        return fedMonkeys;
    }

    public int getInvestDiff() {
        return investDiff;
    }

    public Type getType() {
        return type;
    }

    public String getTxnId() {
        return txnId;
    }

    public static class Serde extends BaseJsonSerde<TraderStateUpdater> {
        public Serde() { super(TraderStateUpdater.class); }
    }

    public TxnResult update(TraderState state) {
        if (state == null) {
            state = TraderState.init();
        }
        TraderState newState = new TraderState(
            state.getCoins() + getCoinsDiff(),
            state.getShares() + getSharesDiff(),
            state.getBailouts() + (getAddBailout() ? 1 : 0),
            state.getFedMonkeys() + getFedMonkeys(),
            state.getInFlightInvestments() + getInvestDiff());

        // Bailout if needed !!!
        if (type != Type.BAILOUT &&
            newState.getInFlightInvestments() <= 0 &&
            newState.getCoins() <= 3 &&
            newState.getShares() <= 0) {
            newState = BAILOUT_UPDATER.update(newState).getState();
        }

        TxnResultType status = newState.validate();
        TraderState keptState = status == TxnResultType.ACCEPTED
            ? newState
            : state;
        return new TxnResult(txnId, type.name(), keptState, status);
    }

    public static TraderStateUpdater from(MarketOrder order, double sharePrice) {
        return new TraderStateUpdater(
            order.getTxnId(),
            Type.MARKET,
            order.getType().getCoinSign() * order.getShares() * sharePrice,
            order.getType().getShareSign() * order.getShares(),
            false,
            0,
            0);
    }

    public static TraderStateUpdater from(Investment investment) {
        return new TraderStateUpdater(
            investment.getTxnId(),
            Type.INVEST,
            -1 * investment.getInvested(),
            0,
            false,
            0,
            1);
    }

    public static TraderStateUpdater from(FeedMonkeys feed) {
        return new TraderStateUpdater(
            feed.getTxnId(),
            Type.FEED,
            0,
            -1 * feed.getMonkeys(),
            false,
            feed.getMonkeys(),
            0);
    }

}
