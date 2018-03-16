package mktd6.flink.trader;

import mktd6.TimestampExtractor;
import mktd6.flink.TraderKeySelector;
import mktd6.model.Team;
import mktd6.model.market.SharePriceInfo;
import mktd6.model.market.ops.TxnResult;
import mktd6.model.trader.Trader;
import mktd6.model.trader.TraderState;
import mktd6.model.trader.ops.*;
import mktd6.serde.flink.JsonSchema;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;

public class MonkeyStockChampion {

    private static final Logger LOG = LoggerFactory.getLogger(MonkeyStockChampion.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: " + MonkeyStockChampion.class.getSimpleName() +
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
            team = Team.CAPUCIN;
            name = "foobar";
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
        props.setProperty("group.id", MonkeyStockChampion.class.getSimpleName()+"_"+ UUID.randomUUID().toString()); // Consumer group ID
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

        // Init order to retrieve state for bootstrapping
        env.fromElements(Tuple2.of(trader, FeedMonkeys.make(trader.getName() + "_Init", 1)))
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        TopicDef.FEED_MONKEYS.getTopicName(),
                        new JsonSchema<>(Trader.class, FeedMonkeys.class)
                ));

        DataStream<Tuple2<String, SharePriceInfo>> sharePrices = env.addSource(sharePricesConsumer);

        // Map incoming share prices to the current trader
        // This will be useful for joining later on
        DataStream<Tuple2<Trader, SharePriceInfo>> traderSelfPrices = sharePrices
                .map(new MapShareInfoToTrader(trader))
                .map(new MapFunction<Tuple2<Trader, SharePriceInfo>, Tuple2<Trader, SharePriceInfo>>() {
                    @Override
                    public Tuple2<Trader, SharePriceInfo> map(Tuple2<Trader, SharePriceInfo> value) throws Exception {
                        LOG.info("Mapping trader to sharePrice");
                        return value;
                    }
                });

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

        SplitStream<Tuple2<Trader, TraderOp>> operations = env.addSource(txnResultsConsumer)
                // Filter self
                .filter(new FilterFunction<Tuple2<Trader, TxnResult>>() {
                    @Override
                    public boolean filter(Tuple2<Trader, TxnResult> value) throws Exception {
                        LOG.info("Is this my state ? {}", value.f0.getTeam() == trader.getTeam() && trader.getName().equals(value.f0.getName()));
                        return value.f0.getTeam() == trader.getTeam() && trader.getName().equals(value.f0.getName());
                    }
                })
                .coGroup(traderSelfPrices)
                .where(new TraderKeySelector<>())
                .equalTo(new TraderKeySelector<>())
                .window(GlobalWindows.create())
                .trigger(new Trigger<CoGroupedStreams.TaggedUnion<Tuple2<Trader, TxnResult>, Tuple2<Trader, SharePriceInfo>>, GlobalWindow>() {
                    @Override
                    public TriggerResult onElement(CoGroupedStreams.TaggedUnion<Tuple2<Trader, TxnResult>, Tuple2<Trader, SharePriceInfo>> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
                        LOG.info("onElement <{}/{}>", element.getOne(), element.getTwo());
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        LOG.info("onProcessingTime");
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        LOG.info("onEventTime");
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
                        LOG.info("clear");
                    }
                })
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Tuple2<Trader, TxnResult>, Tuple2<Trader, SharePriceInfo>, Tuple2<Trader, TraderOp>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Trader, TxnResult>> txnResults, Iterable<Tuple2<Trader, SharePriceInfo>> sharePrices, Collector<Tuple2<Trader, TraderOp>> out) throws Exception {
                        LOG.info("Windowing, bitch");
                        Trader trader = null;
                        TraderState lastState = null;
                        Iterator<Tuple2<Trader, TxnResult>> it = txnResults.iterator();
                        while (it.hasNext()) {
                            Tuple2<Trader, TxnResult> t = it.next();
                            trader = t.f0;
                            lastState = t.f1.getState();
                        }

                        SharePriceInfo lastPrice = null;
                        Iterator<Tuple2<Trader, SharePriceInfo>> iit = sharePrices.iterator();
                        while (iit.hasNext()) {
                            Tuple2<Trader, SharePriceInfo> t = iit.next();
                            lastPrice = t.f1;
                        }

                        if (lastState != null && lastPrice != null) {

                            if (StreamSupport.stream(sharePrices.spliterator(), false)
                                    .map(t -> t.f1)
                                    .allMatch(price -> price.getForecast().getMult() > 1)) {
                                out.collect(Tuple2.of(trader, MarketOrder.make(UUID.randomUUID().toString(), MarketOrderType.BUY, 1)));
                            }

                            if (StreamSupport.stream(sharePrices.spliterator(), false)
                                    .map(t -> t.f1)
                                    .allMatch(price -> price.getForecast().getMult() < 1)) {
                                out.collect(Tuple2.of(trader, MarketOrder.make(UUID.randomUUID().toString(), MarketOrderType.SELL, 1)));
                            }
                            LOG.info("There is content");
                            if (StreamSupport.stream(sharePrices.spliterator(), false)
                                    .map(t -> t.f1)
                                    .allMatch(price -> price.getForecast().getMult() > 1)
                                    && lastState.getCoins() > 10 * lastPrice.getCoins()) {
                                LOG.info("Going up");
                                out.collect(Tuple2.of(trader, MarketOrder.make(UUID.randomUUID().toString(), MarketOrderType.BUY, 3)));
                                out.collect(Tuple2.of(trader, Investment.make(UUID.randomUUID().toString(), 3 * lastPrice.getCoins())));
                                out.collect(Tuple2.of(trader, FeedMonkeys.make(UUID.randomUUID().toString(), 1)));
                            }
                            if (StreamSupport.stream(sharePrices.spliterator(), false)
                                    .map(t -> t.f1)
                                    .allMatch(price -> price.getForecast().getMult() < 1)
                                    && lastState.getCoins() > 10 * lastPrice.getCoins()) {
                                LOG.info("Going down");
                                out.collect(Tuple2.of(trader, FeedMonkeys.make(UUID.randomUUID().toString(), 3)));
                                out.collect(Tuple2.of(trader, Investment.make(UUID.randomUUID().toString(), 1 * lastPrice.getCoins())));
                            }
                            if (lastState.getCoins() < 5 && lastState.getShares() >= 2) {
                                LOG.info("Selling");
                                out.collect(Tuple2.of(trader, MarketOrder.make(UUID.randomUUID().toString(), MarketOrderType.SELL, 2)));
                            }
                        }
                        else {
                            LOG.info("There is no content");
                        }
                    }
                })
                .split(new OutputSelector<Tuple2<Trader, TraderOp>>() {
                    @Override
                    public Iterable<String> select(Tuple2<Trader, TraderOp> value) {
                        List<String> output = Lists.newArrayList();
                        if (value.f1 instanceof MarketOrder) {
                            output.add("market");
                        } else if (value.f1 instanceof FeedMonkeys) {
                            output.add("feed");
                        } else if (value.f1 instanceof Investment) {
                            output.add("investment");
                        }
                        return output;
                    }
                });

                // Send orders
        operations.select("market")
                .map(new MapFunction<Tuple2<Trader,TraderOp>, Tuple2<Trader, MarketOrder>>() {
                    @Override
                    public Tuple2<Trader, MarketOrder> map(Tuple2<Trader, TraderOp> value) throws Exception {
                        LOG.info("Cast into MarketOrder");
                        return Tuple2.of(value.f0, (MarketOrder) value.f1);
                    }
                })
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        TopicDef.MARKET_ORDERS.getTopicName(),
                        new JsonSchema<>(Trader.class, MarketOrder.class)
                ));
        operations.select("feed")
                .map(new MapFunction<Tuple2<Trader,TraderOp>, Tuple2<Trader, FeedMonkeys>>() {
                    @Override
                    public Tuple2<Trader, FeedMonkeys> map(Tuple2<Trader, TraderOp> value) throws Exception {
                        LOG.info("Cast into FeedMonkey");
                        return Tuple2.of(value.f0, (FeedMonkeys) value.f1);
                    }
                })
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        TopicDef.FEED_MONKEYS.getTopicName(),
                        new JsonSchema<>(Trader.class, FeedMonkeys.class)
                ));
        operations.select("investment")
                .map(new MapFunction<Tuple2<Trader,TraderOp>, Tuple2<Trader, Investment>>() {
                    @Override
                    public Tuple2<Trader, Investment> map(Tuple2<Trader, TraderOp> value) throws Exception {
                        LOG.info("Cast into Investment");
                        return Tuple2.of(value.f0, (Investment) value.f1);
                    }
                })
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        TopicDef.INVESTMENT_ORDERS.getTopicName(),
                        new JsonSchema<>(Trader.class, Investment.class)
                ));

        env.execute(MonkeyStockChampion.class.getName());
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
}
