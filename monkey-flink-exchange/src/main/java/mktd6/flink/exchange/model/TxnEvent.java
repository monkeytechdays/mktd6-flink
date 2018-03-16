package mktd6.flink.exchange.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import mktd6.model.market.ops.TxnResult;
import mktd6.serde.kafka.BaseJsonSerde;

public class TxnEvent {

    private final TxnResult txnResult;
    private final double investedCoins;
    private final double totalInvestments;

    @JsonCreator
    public TxnEvent(
            @JsonProperty("txnResult") TxnResult txnResult,
            @JsonProperty("investedCoins") double investedCoins,
            @JsonProperty("totalInvestments") double totalInvestments) {
        this.txnResult= txnResult;
        this.investedCoins = investedCoins;
        this.totalInvestments = totalInvestments;
    }

    public TxnEvent(
            TxnResult txnResult,
            double investedCoins) {
        this.txnResult= txnResult;
        this.investedCoins = investedCoins;
        this.totalInvestments = -1;
    }

    public TxnResult getTxnResult() {
        return txnResult;
    }

    public double getInvestedCoins() {
        return investedCoins;
    }

    public double getTotalInvestments() {
        return totalInvestments;
    }

    public static class Serde extends BaseJsonSerde<TxnEvent> {
        public Serde() { super(TxnEvent.class); }
    }
}
