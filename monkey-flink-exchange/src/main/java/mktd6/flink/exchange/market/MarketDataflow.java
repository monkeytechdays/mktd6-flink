package mktd6.flink.exchange.market;

import mktd6.flink.TraderKeySelector;
import mktd6.flink.exchange.model.ServerTopics;
import mktd6.flink.exchange.model.TraderStateUpdater;
import mktd6.flink.exchange.model.TxnEvent;
import mktd6.model.market.SharePriceInfo;
import mktd6.model.market.ops.TxnResult;
import mktd6.model.market.ops.TxnResultType;
import mktd6.model.trader.Trader;
import mktd6.model.trader.TraderState;
import mktd6.model.trader.ops.FeedMonkeys;
import mktd6.model.trader.ops.Investment;
import mktd6.model.trader.ops.MarketOrder;
import mktd6.serde.flink.JsonSchema;
import mktd6.topic.TopicDef;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static mktd6.flink.LaunchHelper.getLocalIp;

public class MarketDataflow {
    private static final Logger LOG = LoggerFactory.getLogger(MarketDataflow.class);

    public static final String KAFKA_HOST = getLocalIp();
    public static final String KAFKA_PORT = "9092";


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: " + MarketDataflow.class.getSimpleName() +
                "[--bootstrap-servers <bootstrap-servers>]");

        String bootstrapServers;
        if (params.has("bootstrap-servers")
                ) {
            bootstrapServers = params.get("bootstrap-servers");
        } else {
            bootstrapServers = KAFKA_HOST + ":" + KAFKA_PORT;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", bootstrapServers);
        consumerProperties.setProperty("group.id", MarketDataflow.class.getSimpleName()+"_"+ UUID.randomUUID().toString());
        consumerProperties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<Tuple2<Trader, MarketOrder>> marketOrders = env
                .addSource(new FlinkKafkaConsumer011<>(
                        TopicDef.MARKET_ORDERS.getTopicName(),
                        new JsonSchema<>(Trader.class, MarketOrder.class),
                        consumerProperties
                ));

        DataStreamSource<Tuple2<String, SharePriceInfo>> sharePrices = env
                .addSource(new FlinkKafkaConsumer011<>(
                        TopicDef.SHARE_PRICE.getTopicName(),
                        new JsonSchema<>(String.class, SharePriceInfo.class),
                        consumerProperties
                ));

        DataStream<Tuple2<Trader, TraderStateUpdater>> orderStateUpdaters = marketOrders
                .connect(sharePrices)
                .keyBy(new KeySelector<Tuple2<Trader, MarketOrder>, String>() {
                    @Override
                    public String getKey(Tuple2<Trader, MarketOrder> value) throws Exception {
                        return "FOO";
                    }
                }, new KeySelector<Tuple2<String,SharePriceInfo>, String>() {
                    @Override
                    public String getKey(Tuple2<String, SharePriceInfo> value) throws Exception {
                        return "FOO";
                    }
                })
                .flatMap(new StatefulOrderAndSideInputPrice());

        DataStream<Tuple2<Trader, TraderStateUpdater>> feedMonkeysStateUpdaters = env
                .addSource(new FlinkKafkaConsumer011<>(
                        TopicDef.FEED_MONKEYS.getTopicName(),
                        new JsonSchema<>(Trader.class, FeedMonkeys.class),
                        consumerProperties
                ))
                .map(new MapFeedMonkeyToTraderStateUpdate());

        DataStream<Tuple2<Trader, TraderStateUpdater>> investmentsStateUpdaters = env
                .addSource(new FlinkKafkaConsumer011<>(
                        TopicDef.INVESTMENT_ORDERS.getTopicName(),
                        new JsonSchema<>(Trader.class, Investment.class),
                        consumerProperties
                ))
                .map(new MapInvestmentToTraderStateUpdate());

        orderStateUpdaters
                .union(feedMonkeysStateUpdaters)
                .union(investmentsStateUpdaters)
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        ServerTopics.TRADER_UPDATES.getTopicName(),
                        new JsonSchema<>(Trader.class, TraderStateUpdater.class)
                ));

        DataStream<Tuple2<Trader, TxnEvent>> txnEvents = env
                .addSource(new FlinkKafkaConsumer011<>(
                        ServerTopics.TRADER_UPDATES.getTopicName(),
                        new JsonSchema<>(Trader.class, TraderStateUpdater.class),
                        consumerProperties
                ))
                .keyBy(new TraderKeySelector<>())
                .map(new UpdateTraderState());

        DataStream<Tuple2<Trader, TxnEvent>> acceptedtxnEvents = txnEvents
                .filter(new FilterIsAcceptedInvestment())
                .keyBy(new TraderKeySelector<>())
                .map(new ComputeTotalInvestments());

        acceptedtxnEvents.addSink(new FlinkKafkaProducer011<>(
                bootstrapServers,
                ServerTopics.INVESTMENT_TXN_EVENTS.getTopicName(),
                new JsonSchema<>(Trader.class, TxnEvent.class)
        ));

        acceptedtxnEvents.keyBy(new TransactionIdSelector())
                .process(new ProcessReturnOnInvestment())
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        ServerTopics.TRADER_UPDATES.getTopicName(),
                        new JsonSchema<>(Trader.class, TraderStateUpdater.class)
                ));

        txnEvents.map(new MapTransactionEventToResult())
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        TopicDef.TXN_RESULTS.getTopicName(),
                        new JsonSchema<>(Trader.class, TxnResult.class)
                ));

        txnEvents.filter(new FilterAcceptedTransaction())
                .map(new MapTxnEventToTraderState())
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        ServerTopics.TRADER_STATES.getTopicName(),
                        new JsonSchema<>(Trader.class, TraderState.class)
                ));

        env.execute(MarketDataflow.class.getSimpleName());
    }

    private static boolean isAcceptedInvestment(TxnEvent v) {
        return v.getTxnResult().getStatus() == TxnResultType.ACCEPTED
                && v.getTxnResult().getType().equals(TraderStateUpdater.Type.INVEST.name());
    }

    private static class StatefulOrderAndSideInputPrice extends RichCoFlatMapFunction<Tuple2<Trader, MarketOrder>, Tuple2<String, SharePriceInfo>, Tuple2<Trader, TraderStateUpdater>> {

        private MapState<Trader, PriorityQueue<MarketOrder>> ordersStates;
        private ValueState<Double> sharePrice;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> hypeDescriptor =
                    new ValueStateDescriptor<>(
                            "sharePrice",
                            BasicTypeInfo.DOUBLE_TYPE_INFO);
            sharePrice = getRuntimeContext().getState(hypeDescriptor);
            ordersStates = getRuntimeContext().getMapState(new MapStateDescriptor<Trader, PriorityQueue<MarketOrder>>(
                    "marketOrders",
                    TypeInformation.of(Trader.class),
                    TypeInformation.of(new TypeHint<PriorityQueue<MarketOrder>>() {
                    }))
            );
        }

        @Override
        public void flatMap1(Tuple2<Trader, MarketOrder> value, Collector<Tuple2<Trader, TraderStateUpdater>> out) throws Exception {

            PriorityQueue<MarketOrder> queue = ordersStates.get(value.f0);
            if (sharePrice.value() == null) {
                if (queue == null) {
                    queue = new PriorityQueue<>(10, new Comparator<MarketOrder>() {
                        @Override
                        public int compare(MarketOrder o1, MarketOrder o2) {
                            return Long.compare(o1.getTime().getMillis(),
                                    o2.getTime().getMillis());
                        }
                    });
                }
                queue.add(value.f1);
                ordersStates.put(value.f0, queue);
            } else {
                if (queue != null) {
                    queue.add(value.f1);
                    MarketOrder order = queue.peek();
                    while (order != null) {
                        out.collect(Tuple2.of(value.f0, TraderStateUpdater.from(order, sharePrice.value())));
                        queue.remove(order);
                        order = queue.peek();
                    }
                    ordersStates.remove(value.f0);
                } else {
                    out.collect(Tuple2.of(value.f0, TraderStateUpdater.from(value.f1, sharePrice.value())));
                }
            }
        }

        @Override
        public void flatMap2(Tuple2<String, SharePriceInfo> value, Collector<Tuple2<Trader, TraderStateUpdater>> out) throws Exception {
            if (sharePrice.value()==null && ordersStates.entries()!=null) {

                Set<Trader> remove = new HashSet<>();

                for (Map.Entry<Trader, PriorityQueue<MarketOrder>> entry : ordersStates.entries()) {
                    PriorityQueue<MarketOrder> queue = entry.getValue();
                    if (queue != null) {
                        MarketOrder order = queue.peek();
                        while (order != null) {
                            out.collect(Tuple2.of(entry.getKey(), TraderStateUpdater.from(order, value.f1.getCoins())));
                            queue.remove(order);
                            order = queue.peek();
                        }
                    }
                    remove.add(entry.getKey());
                }

                for(Trader trader : remove) {
                    ordersStates.remove(trader);
                }
            }

            sharePrice.update(value.f1.getCoins());
        }
    }

    private static class MapFeedMonkeyToTraderStateUpdate implements MapFunction<Tuple2<Trader, FeedMonkeys>, Tuple2<Trader, TraderStateUpdater>> {
        @Override
        public Tuple2<Trader, TraderStateUpdater> map(Tuple2<Trader, FeedMonkeys> value) throws Exception {
            return Tuple2.of(value.f0, TraderStateUpdater.from(value.f1));
        }
    }

    private static class MapInvestmentToTraderStateUpdate implements MapFunction<Tuple2<Trader, Investment>, Tuple2<Trader, TraderStateUpdater>> {
        @Override
        public Tuple2<Trader, TraderStateUpdater> map(Tuple2<Trader, Investment> value) throws Exception {
            return Tuple2.of(value.f0, TraderStateUpdater.from(value.f1));
        }
    }

    private static class UpdateTraderState extends RichMapFunction<Tuple2<Trader, TraderStateUpdater>, Tuple2<Trader, TxnEvent>> {
        private ValueState<TraderState> traderState;

        @Override
        public void open(Configuration parameters) throws Exception {
            traderState = getRuntimeContext().getState(new ValueStateDescriptor<TraderState>(
                    "traderState",
                    TraderState.class
            ));
        }

        @Override
        public Tuple2<Trader, TxnEvent> map(Tuple2<Trader, TraderStateUpdater> value) throws Exception {
            Trader trader = value.f0;
            TraderStateUpdater upd = value.f1;
            TraderState state = Optional.ofNullable(traderState.value()).orElse(TraderState.init());
            TxnResult result = upd.update(state);
            double investedCoins =
                    result.getStatus() == TxnResultType.ACCEPTED &&
                            upd.getType() == TraderStateUpdater.Type.INVEST
                            ? Math.abs(upd.getCoinsDiff())
                            : 0;
            TxnEvent event = new TxnEvent(result, investedCoins);
            traderState.update(result.getState());
            return Tuple2.of(trader, event);
        }
    }

    private static class FilterIsAcceptedInvestment implements FilterFunction<Tuple2<Trader, TxnEvent>> {
        @Override
        public boolean filter(Tuple2<Trader, TxnEvent> value) throws Exception {
            return isAcceptedInvestment(value.f1);
        }
    }

    private static class ComputeTotalInvestments extends RichMapFunction<Tuple2<Trader, TxnEvent>, Tuple2<Trader, TxnEvent>> {

        private ValueState<Double> totalInvestments;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalInvestments = getRuntimeContext().getState(new ValueStateDescriptor<Double>(
                    "totalInvestments",
                    BasicTypeInfo.DOUBLE_TYPE_INFO
            ));
        }

        @Override
        public Tuple2<Trader, TxnEvent> map(Tuple2<Trader, TxnEvent> value) throws Exception {
            double newInvestment = value.f1.getInvestedCoins();
            double prevInvestments = Optional.ofNullable(totalInvestments.value()).orElse(0d);
            double total = Math.abs(newInvestment) + prevInvestments;
            totalInvestments.update(total);
            return Tuple2.of(value.f0, new TxnEvent(value.f1.getTxnResult(), value.f1.getInvestedCoins(), total));
        }
    }

    private static class TransactionIdSelector implements KeySelector<Tuple2<Trader,TxnEvent>, String> {
        @Override
        public String getKey(Tuple2<Trader, TxnEvent> value) throws Exception {
            return value.f1.getTxnResult().getTxnId();
        }
    }

    private static class ProcessReturnOnInvestment extends ProcessFunction<Tuple2<Trader,TxnEvent>, Tuple2<Trader, TraderStateUpdater>> {
        private ValueState<Tuple3<Trader, String, Double>> txnReturnState;

        @Override
        public void open(Configuration parameters) throws Exception {
            txnReturnState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "txnReturnState",
                    TypeInformation.of(new TypeHint<Tuple3<Trader, String, Double>>() {
                    })));
        }

        @Override
        public void processElement(Tuple2<Trader, TxnEvent> value, Context ctx, Collector<Tuple2<Trader, TraderStateUpdater>> out) throws Exception {
            TimerService timerService = ctx.timerService();

            Trader trader = value.f0;
            TxnEvent event = value.f1;
            TxnResult result = event.getTxnResult();

            double totalInvestments = event.getTotalInvestments();
            long timeToWait = (long) totalInvestments;
            double logNormalBiasReturn = Math.exp(-1 - (totalInvestments / 1000d));
            LogNormalDistribution logNormal = new LogNormalDistribution(0.035 + logNormalBiasReturn, 0.01);
            double investmentReturn = logNormal.sample();

            txnReturnState.update(Tuple3.of(trader, result.getTxnId(), investmentReturn * event.getInvestedCoins()));

            timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + timeToWait);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Trader, TraderStateUpdater>> out) throws Exception {
            Tuple3<Trader, String, Double> txnReturn = txnReturnState.value();
            out.collect(Tuple2.of(txnReturn.f0,
                    new TraderStateUpdater(
                            txnReturn.f1,
                            TraderStateUpdater.Type.RETURN,
                            txnReturn.f2,
                            0,
                            false,
                            0,
                            -1
                    )));
        }
    }

    private static class MapTransactionEventToResult implements MapFunction<Tuple2<Trader,TxnEvent>, Tuple2<Trader, TxnResult>> {
        @Override
        public Tuple2<Trader, TxnResult> map(Tuple2<Trader, TxnEvent> value) throws Exception {
            return Tuple2.of(value.f0, value.f1.getTxnResult());
        }
    }

    private static class FilterAcceptedTransaction implements FilterFunction<Tuple2<Trader, TxnEvent>> {
        @Override
        public boolean filter(Tuple2<Trader, TxnEvent> value) throws Exception {
            return value.f1.getTxnResult().getStatus() == TxnResultType.ACCEPTED;
        }
    }

    private static class MapTxnEventToTraderState implements MapFunction<Tuple2<Trader,TxnEvent>, Tuple2<Trader, TraderState>> {
        @Override
        public Tuple2<Trader, TraderState> map(Tuple2<Trader, TxnEvent> value) throws Exception {
            return Tuple2.of(value.f0, value.f1.getTxnResult().getState());
        }
    }
}
