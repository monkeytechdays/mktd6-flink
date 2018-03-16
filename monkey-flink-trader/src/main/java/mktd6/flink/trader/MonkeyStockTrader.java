package mktd6.flink.trader;

import mktd6.TimestampExtractor;
import mktd6.flink.TraderKeySelector;
import mktd6.model.Team;
import mktd6.model.market.SharePriceInfo;
import mktd6.model.market.ops.TxnResult;
import mktd6.model.trader.Trader;
import mktd6.model.trader.ops.Investment;
import mktd6.model.trader.ops.MarketOrder;
import mktd6.model.trader.ops.MarketOrderType;
import mktd6.serde.flink.JsonSchema;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

public class MonkeyStockTrader {

    private static final Logger LOG = LoggerFactory.getLogger(MonkeyStockTrader.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: " + MonkeyStockTrader.class.getSimpleName() +
                "[--bootstrap-servers <bootstrap-servers> --team <team> --name <name>]");

        Team team;
        String name;
        String bootstrapServers;
        if (params.has("team")
                && params.has("name")
                && params.has("bootstrap-servers")
                ) {
            team = Team.valueOf(params.get("team"));
            name = params.get("name");
            bootstrapServers = params.get("bootstrap-servers");
        } else {
            team = Team.ALOUATE;
            name = "dummy";
            bootstrapServers = "localhost:9092";
        }

        final Trader trader = new Trader(team, name);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers); // Broker default host:port
        props.setProperty("group.id", MonkeyStockTrader.class.getSimpleName()+"_"+ UUID.randomUUID().toString()); // Consumer group ID
        props.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer011<Tuple2<String, SharePriceInfo>> sharePricesConsumer =
                new FlinkKafkaConsumer011<>(
                        TopicDef.SHARE_PRICE.getTopicName(),
                        new JsonSchema<>(String.class, SharePriceInfo.class),
                        props
                );
        sharePricesConsumer.setStartFromGroupOffsets();
        sharePricesConsumer.setCommitOffsetsOnCheckpoints(true);
        sharePricesConsumer.assignTimestampsAndWatermarks(new TimestampExtractor<>());

        DataStream<Tuple2<String, SharePriceInfo>> sharePrices = env.addSource(sharePricesConsumer);

        // Look at the price forecast and follow the advice:
        // - if the forecast is > 1, meaning the price should increase,
        //   then BUY 1 share,
        // - if the forecast is < 1, meaning the price should decrease,
        //   then SELL 1 share.
        // Don't even care about looking at our assets, let the
        // market accept/reject the transaction.
        sharePrices
                // Stream should be keyed in order to work with state
                .keyBy(0)
                // Buy or sell
                .map(new FollowForecast(trader))
                // Send orders
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        TopicDef.MARKET_ORDERS.getTopicName(),
                        new JsonSchema<>(Trader.class, MarketOrder.class)
                ));

        // Map incoming share prices to the current trader
        // This will be useful for joining later on
        DataStream<Tuple2<Trader, SharePriceInfo>> traderSelfPrices = sharePrices
                .map(new MapShareInfoToTrader(trader));

        // Configure transaction consumer
        FlinkKafkaConsumer011<Tuple2<Trader, TxnResult>> txnResultsConsumer =
                new FlinkKafkaConsumer011<>(
                        TopicDef.TXN_RESULTS.getTopicName(),
                        new JsonSchema<>(Trader.class, TxnResult.class),
                        props
                );
        txnResultsConsumer.setStartFromGroupOffsets();
        txnResultsConsumer.setCommitOffsetsOnCheckpoints(true);
        // Custom timestamp assigner: read time from inner TraderState object
        txnResultsConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Trader, TxnResult>>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(Tuple2<Trader, TxnResult> element) {
                return element.f1.getState().getTime().getMillis();
            }
        });

        env.addSource(txnResultsConsumer)
                // Map transaction to <trader,coins>
                .map(new MapTraderCoins())
                // Connect with <trader,share-prices>
                .connect(traderSelfPrices)
                // Connect on <trader> key, must serializable friendly String
                .keyBy(new TraderKeySelector<>(), new TraderKeySelector<>())
                // Keep only most recent share-price and emit trader coins prediction
                // when trader-state arrive
                .flatMap(new TraderStatePriceMatcher())
                // Go forth only if predicted state is be valid (can invest)
                .filter(new TraderHasCoins())
                // Stream should be keyed in order to work with state
                .keyBy(new TraderKeySelector<>())
                // Create investment according to prediction
                .map(new MakeInvestment())
                // Send orders
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        TopicDef.INVESTMENT_ORDERS.getTopicName(),
                        new JsonSchema<>(Trader.class, Investment.class)
                ));

        env.execute(MonkeyStockTrader.class.getName());
    }

    private static class FollowForecast extends RichMapFunction<Tuple2<String,SharePriceInfo>, Tuple2<Trader, MarketOrder>> {

        private final Trader trader;
        private ValueState<Integer> txnId;

        public FollowForecast(Trader trader) {
            this.trader = trader;
        }

        @Override
        public Tuple2<Trader, MarketOrder> map(Tuple2<String, SharePriceInfo> infoTuple) throws Exception {
            Integer id = txnId.value();
            if(id == null) {
                id = 1;
            }
            MarketOrderType type = infoTuple.f1.getForecast().getMult() > 1
                    ? MarketOrderType.BUY
                    : MarketOrderType.SELL;
            LOG.info("Trader order: {} 1 share at {}", type, infoTuple.f1.getCoins());
            id++;

            txnId.update(id);

            return Tuple2.of(
                    trader,
                    MarketOrder.make(trader.getName() + "_Order_" + id, type, 1)
            );
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "txnId",
                            // type information of state
                            BasicTypeInfo.INT_TYPE_INFO);
            txnId = getRuntimeContext().getState(descriptor);
        }
    }

    private static class TraderStatePriceMatcher extends RichCoFlatMapFunction<Tuple2<Trader, Double>, Tuple2<Trader, SharePriceInfo>, Tuple2<Trader, Double>> {

        private ValueState<Double> coins;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "coins",
                            // type information of state
                            BasicTypeInfo.DOUBLE_TYPE_INFO);
            coins = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap1(Tuple2<Trader, Double> coinsTuple, Collector<Tuple2<Trader, Double>> out) throws Exception {
            LOG.info("Current trader coins: <{}, {}>", coinsTuple.f0.getName(), coinsTuple.f1);
            if (coins.value()!=null) {
                out.collect(Tuple2.of(coinsTuple.f0, coinsTuple.f1 - coins.value()));
            }
        }

        @Override
        public void flatMap2(Tuple2<Trader, SharePriceInfo> priceTuple, Collector<Tuple2<Trader, Double>> out) throws Exception {
            LOG.info("Current share price: <{}, {}>", priceTuple.f0.getName(), priceTuple.f1.getCoins());
            coins.update(priceTuple.f1.getCoins());
        }
    }

    private static class MakeInvestment extends RichMapFunction<Tuple2<Trader, Double>, Tuple2<Trader, Investment>> {

        private ValueState<Integer> txnId;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "txnId",
                            // type information of state
                            BasicTypeInfo.INT_TYPE_INFO);
            txnId = getRuntimeContext().getState(descriptor);
        }

        @Override
        public Tuple2<Trader, Investment> map(Tuple2<Trader, Double> traderCoinsTuple) throws Exception {
            Integer id = txnId.value();
            if(id == null) {
                id = 1;
            }

            LOG.info("Investment order: {}", traderCoinsTuple.f1);
            id++;

            txnId.update(id);

            return Tuple2.of(
                    traderCoinsTuple.f0,
                    Investment.make(traderCoinsTuple.f0.getName() + "_Investment_" + id, traderCoinsTuple.f1)
            );
        }
    }

    private static class MapShareInfoToTrader implements MapFunction<Tuple2<String, SharePriceInfo>, Tuple2<Trader, SharePriceInfo>> {
        private final Trader trader;

        public MapShareInfoToTrader(Trader trader) {
            this.trader = trader;
        }

        @Override
        public Tuple2<Trader, SharePriceInfo> map(Tuple2<String, SharePriceInfo> infoTuple) throws Exception {
            return Tuple2.of(trader, infoTuple.f1);
        }
    }

    private static class MapTraderCoins implements MapFunction<Tuple2<Trader,TxnResult>, Tuple2<Trader, Double>> {
        @Override
        public Tuple2<Trader, Double> map(Tuple2<Trader, TxnResult> txnResultTuple) throws Exception {
            LOG.info("Transaction received: <{},{}>", txnResultTuple.f0.getName(), Joiner.on(",").join(Lists.newArrayList(txnResultTuple.f1.getTxnId(), txnResultTuple.f1.getType(), txnResultTuple.f1.getStatus().name(), txnResultTuple.f1.getState().getCoins())));
            return Tuple2.of(txnResultTuple.f0, txnResultTuple.f1.getState().getCoins());
        }
    }

    private static class TraderHasCoins implements FilterFunction<Tuple2<Trader, Double>> {
        @Override
        public boolean filter(Tuple2<Trader, Double> traderCoinsTuple) throws Exception {
            return traderCoinsTuple.f1 > 0;
        }
    }
}
