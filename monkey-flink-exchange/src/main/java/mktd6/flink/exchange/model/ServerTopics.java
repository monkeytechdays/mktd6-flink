package mktd6.flink.exchange.model;

import mktd6.model.trader.Trader;
import mktd6.model.trader.TraderState;
import mktd6.serde.kafka.JsonSerde;
import mktd6.topic.TopicDef;

public class ServerTopics {

    public static final TopicDef<String, ShareHypePiece> SHARE_HYPE =
        new TopicDef<>(
            "share-hype",
            new JsonSerde.StringSerde(),
            new ShareHypePiece.Serde());

    public static final TopicDef<Trader, TraderStateUpdater> TRADER_UPDATES =
        new TopicDef<>(
            "trader-state-updates",
            new JsonSerde.TraderSerde(),
            new TraderStateUpdater.Serde());

    public static final TopicDef<Trader, TraderState> TRADER_STATES = new TopicDef<>(
            "trader-states",
            new JsonSerde.TraderSerde(),
            new JsonSerde.TraderStateSerde());

    public static final TopicDef<Trader, TxnEvent> INVESTMENT_TXN_EVENTS = new TopicDef<>(
            "investment-txn-events",
            new JsonSerde.TraderSerde(),
            new TxnEvent.Serde());

}
