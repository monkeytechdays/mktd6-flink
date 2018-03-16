package mktd6.serde.kafka;

import mktd6.model.gibber.Gibb;
import mktd6.model.market.SharePriceInfo;
import mktd6.model.market.SharePriceMult;
import mktd6.model.market.ops.TxnResult;
import mktd6.model.trader.Trader;
import mktd6.model.trader.TraderState;
import mktd6.model.trader.ops.FeedMonkeys;
import mktd6.model.trader.ops.Investment;
import mktd6.model.trader.ops.MarketOrder;

public class JsonSerde {

    public static class StringSerde extends BaseJsonSerde<String> {
        public StringSerde() { super(String.class); }
    }
    public static class DoubleSerde extends BaseJsonSerde<Double> {
        public DoubleSerde() { super(Double.class); }
    }
    public static class VoidSerde extends BaseJsonSerde<Void> {
        public VoidSerde() { super(Void.class); }
    }

    public static class TraderSerde extends BaseJsonSerde<Trader> {
        public TraderSerde() { super(Trader.class); }
    }
    public static class TraderStateSerde extends BaseJsonSerde<TraderState> {
        public TraderStateSerde() { super(TraderState.class); }
    }
    public static class MarketOrderSerde extends BaseJsonSerde<MarketOrder> {
        public MarketOrderSerde() { super(MarketOrder.class); }
    }
    public static class InvestmentSerde extends BaseJsonSerde<Investment> {
        public InvestmentSerde() { super(Investment.class); }
    }
    public static class FeedMonkeysSerde extends BaseJsonSerde<FeedMonkeys> {
        public FeedMonkeysSerde() { super(FeedMonkeys.class); }
    }
    public static class TxnResultSerde extends BaseJsonSerde<TxnResult> {
        public TxnResultSerde() { super(TxnResult.class); }
    }
    public static class SharePriceInfoSerde extends BaseJsonSerde<SharePriceInfo> {
        public SharePriceInfoSerde() { super(SharePriceInfo.class); }
    }
    public static class SharePriceMultSerde extends BaseJsonSerde<SharePriceMult> {
        public SharePriceMultSerde() { super(SharePriceMult.class); }
    }
    public static class GibbSerde extends BaseJsonSerde<Gibb> {
        public GibbSerde() { super(Gibb.class); }
    }
}
