package mktd6.flink.exchange.priceinfo;

import mktd6.flink.exchange.model.BurstStep;
import mktd6.flink.exchange.model.ServerTopics;
import mktd6.flink.exchange.model.ShareHypePiece;
import mktd6.model.gibber.Gibb;
import mktd6.model.market.SharePriceInfo;
import mktd6.model.market.SharePriceMult;
import mktd6.serde.flink.JsonSchema;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static mktd6.flink.LaunchHelper.getLocalIp;

public class SharePriceDataflow {
    private static final Logger LOG = LoggerFactory.getLogger(SharePriceDataflow.class);

    public static final String KAFKA_HOST = getLocalIp();
    public static final String KAFKA_PORT = "9092";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: " + SharePriceDataflow.class.getSimpleName() +
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

        DataStream<Tuple2<String, Double>> sharePriceBase = env
                .addSource(new SharePriceMultSource())
                .map(new AssignKeyToMult())
                .keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
                    private ValueState<Double> multState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Double> priceDescriptor =
                                new ValueStateDescriptor<>(
                                        "mult",
                                        BasicTypeInfo.DOUBLE_TYPE_INFO);
                        multState = getRuntimeContext().getState(priceDescriptor);
                    }

                    @Override
                    public Tuple2<String, Double> map(Tuple2<String, Double> priceMult) throws Exception {
                        Double mult = multState.value();
                        if (mult == null) {
                            multState.update(1d);
                        }

                        Double newValue = multState.value() * priceMult.f1;
                        multState.update(newValue);

                        return Tuple2.of(priceMult.f0, newValue);
                    }
                });

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", bootstrapServers);
        consumerProperties.setProperty("group.id", SharePriceDataflow.class.getSimpleName()+"_"+ UUID.randomUUID().toString());
        consumerProperties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<Tuple2<String, Gibb>> gibbs = env
                .addSource(new FlinkKafkaConsumer011<>(
                        TopicDef.GIBBS.getTopicName(),
                        new JsonSchema<>(String.class, Gibb.class),
                        consumerProperties
                ));

        // Compute the hype
        DataStream<Tuple2<String, ShareHypePiece>> hype = gibbs
                .filter(new FilterBananaGibbs())
                .flatMap(new MapGibbToHype());
        hype.addSink(new FlinkKafkaProducer011<>(
                bootstrapServers,
                ServerTopics.SHARE_HYPE.getTopicName(),
                new JsonSchema<>(String.class, ShareHypePiece.class)
        ));

        // Compute the total hype influence
        hype.map(new MapHypeInfluence()).keyBy(0)
                .sum("f1")
                .keyBy(0)
                .map(new MapPriceWithBubbleBurst())
                .connect(sharePriceBase)
                .keyBy(0, 0)
                .flatMap(new MapEmaAndPrice())
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        TopicDef.SHARE_PRICE.getTopicName(),
                        new JsonSchema<>(String.class, SharePriceInfo.class)
                ));

        env.execute(SharePriceDataflow.class.getSimpleName());
    }

    private static class AssignKeyToMult implements MapFunction<SharePriceMult, Tuple2<String, Double>> {
        @Override
        public Tuple2<String, Double> map(SharePriceMult value) throws Exception {
            LOG.info("Mult: {}", value.getMult());
            return Tuple2.of("FOO", value.getMult());
        }
    }

    private static class FilterBananaGibbs implements FilterFunction<Tuple2<String, Gibb>> {
        @Override
        public boolean filter(Tuple2<String, Gibb> gibbTuple) throws Exception {
            return gibbTuple.f1.getText().contains("banana");
        }
    }

    private static class MapGibbToHype implements FlatMapFunction<Tuple2<String, Gibb>, Tuple2<String, ShareHypePiece>> {
        @Override
        public void flatMap(Tuple2<String, Gibb> gibbTuple, Collector<Tuple2<String, ShareHypePiece>> out) throws Exception {
            String key = gibbTuple.f0;
            for (ShareHypePiece piece : ShareHypePiece.hypePieces(gibbTuple.f1)) {
                out.collect(Tuple2.of(key, piece));
            }
        }
    }

    private static class MapHypeInfluence implements MapFunction<Tuple2<String, ShareHypePiece>, Tuple2<String, Double>> {
        @Override
        public Tuple2<String, Double> map(Tuple2<String, ShareHypePiece> hypeTuple) throws Exception {
            return Tuple2.of(hypeTuple.f0, hypeTuple.f1.getInfluence() * 0.01d);
        }
    }

    private static class MapPriceWithBubbleBurst extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {

        private ValueState<Double> priceState;
        private ValueState<String> burstState;
        private final Random random = new Random();

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> priceDescriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            "price",
                            // type information of state
                            BasicTypeInfo.DOUBLE_TYPE_INFO);
            priceState = getRuntimeContext().getState(priceDescriptor);
            ValueStateDescriptor<String> burstDescriptor =
                    new ValueStateDescriptor<>(
                            "burst",
                            BasicTypeInfo.STRING_TYPE_INFO);
            burstState = getRuntimeContext().getState(burstDescriptor);
        }

        @Override
        public Tuple2<String, Double> map(Tuple2<String, Double> hypeInfluenceTuple) throws Exception {
            String key = hypeInfluenceTuple.f0;
            double value = hypeInfluenceTuple.f1;

            Double price = priceState.value();
            if (price==null) {
                priceState.update(0d);
            }

            double bursts = priceState.value();
            double diff = value - bursts;

            String stepId = burstState.value();

            if (stepId != null) {
                BurstStep step = BurstStep.valueOf(stepId);
                double burstDiff = diff * step.getMult();
                double bursted = diff - burstDiff;
                bursts += bursted;
                diff = burstDiff;
                LOG.info("Bubble burst!!! {} - x{} (bursted: {})", step.name(), step.getMult(), bursted);
                priceState.update(bursts);
                burstState.update(step.getNext().map(Enum::name).orElse(null));
            }
            else if (random.nextDouble() < diff * 0.01) {
                // The more hype, the more risk of the burst of a hype bubble...
                burstState.update(BurstStep.STEP1.name());
            }

            priceState.update(diff);
            LOG.info(String.format("Influence: %.5f - %.5f = %.5f", value, bursts, diff));
            return Tuple2.of(key, diff);
        }
    }

    private static class MapEmaAndPrice extends RichCoFlatMapFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, SharePriceInfo>> {

        private ValueState<Double> ema;
        private ValueState<Double> hypePrice;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> hypeDescriptor =
                    new ValueStateDescriptor<>(
                            "hype",
                            BasicTypeInfo.DOUBLE_TYPE_INFO);
            hypePrice = getRuntimeContext().getState(hypeDescriptor);
            ValueStateDescriptor<Double> emaDescriptor =
                    new ValueStateDescriptor<>(
                            "ema",
                            BasicTypeInfo.DOUBLE_TYPE_INFO);
            ema = getRuntimeContext().getState(emaDescriptor);
        }

        @Override
        public void flatMap1(Tuple2<String, Double> value, Collector<Tuple2<String, SharePriceInfo>> out) throws Exception {
            hypePrice.update(value.f1);
        }

        @Override
        public void flatMap2(Tuple2<String, Double> value, Collector<Tuple2<String, SharePriceInfo>> out) throws Exception {
            double factor = 0.1d;

            double hypeComponent = Optional.ofNullable(hypePrice.value()).orElse(0d);
            double newValue = value.f1 + hypeComponent;

            double previousValue = Optional.ofNullable(ema.value()).orElse(newValue);
            double movingAverage = previousValue * (1d - factor) + newValue * factor;
            ema.update(movingAverage);
            double forecastMult = movingAverage / newValue;
            SharePriceInfo result = SharePriceInfo.make(newValue, forecastMult);
            LOG.info("PriceInfo: {}", newValue);
            out.collect(Tuple2.of(value.f0, result));
        }
    }
}
