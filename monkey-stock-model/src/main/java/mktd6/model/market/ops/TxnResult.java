package mktd6.model.market.ops;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import mktd6.model.trader.TraderState;

public class TxnResult {

    private final String txnId;
    private final String type;
    private final TraderState state;
    private final TxnResultType status;

    @JsonCreator
    public TxnResult(
            @JsonProperty("txnId") String txnId,
            @JsonProperty("type") String type,
            @JsonProperty("state") TraderState state,
            @JsonProperty("status") TxnResultType status
    ) {
        this.txnId = txnId;
        this.type = type;
        this.state = state;
        this.status = status;
    }

    public String getTxnId() {
        return txnId;
    }

    public String getType() {
        return type;
    }

    public TraderState getState() {
        return state;
    }

    public TxnResultType getStatus() {
        return status;
    }
}
