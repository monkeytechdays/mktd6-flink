package mktd6.flink.katas;

import mktd6.TimestampExtractor;
import mktd6.flink.katas.testutils.EmbeddedClustersBoilerplate;
import mktd6.model.market.SharePriceInfo;
import mktd6.serde.flink.JsonSchema;
import mktd6.serde.kafka.JsonSerde;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Until now, we have not used any external state in our exercises.
 *
 * <p>The exercise this time will be to compute a moving average on
 * share prices, using a state store.</p>
 *
 * For more information on state management, please read this
 * <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/stream/state/state.html">article</a>.
 *
 */
public class Chapter04_WorkingWithState extends EmbeddedClustersBoilerplate<String, SharePriceInfo, Double> {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter04_WorkingWithState.class);

    //==========================================================================
    //==== ASSETS

    private static final TopicDef<String, SharePriceInfo> PRICES = TopicDef.SHARE_PRICE;

    private static final TopicDef<String, Double> PRICE_EMA = new TopicDef<>(
            "price-ema",
            new JsonSerde.StringSerde(),
            new JsonSerde.DoubleSerde()
    );

    public Chapter04_WorkingWithState() {
        super(PRICES, PRICE_EMA);
    }

    //==========================================================================
    //==== FIXME FLINK DATAFLOW

    /**
     * We want to compute an
     * <a href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">
     * exponential moving average</a> of prices.
     *
     * <p>We receive price info from the {@link #PRICES} topic, and use a
     * {@link ValueState<Double>} class to compute the EMA.</p>
     */
    @Override
    protected DataStream<Tuple2<String, Double>> buildFlinkDataflow(DataStream<Tuple2<String, SharePriceInfo>> source) {
        // >>> Your job starts here.

        // ######## Read the javadoc !!! ########

        // Summary:
        // stream data from the PRICES topic
        // group the stream by key
        // use a RichFlatMapFunction to initialise the ValueState in its open method
        //
        // compute the EMA using the stateful ema inside the flatMap method
        // collect the EMA values to the PRICE_EMA topic

        // <<< Your job ends here.
        return source
                .keyBy(0)
                .flatMap(new EMATransformer());
    }
    private class EMATransformer extends RichFlatMapFunction<Tuple2<String, SharePriceInfo>, Tuple2<String, Double>> {

        private ValueState<Double> emaState;

        @Override
        public void flatMap(Tuple2<String, SharePriceInfo> value, Collector<Tuple2<String, Double>> out) throws Exception {
            Double currentEma = emaState.value();

            currentEma = computeEma(currentEma, value.f1);

            emaState.update(currentEma);

            out.collect(Tuple2.of(EMA_STATE_KEY, currentEma));
        }

        @Override
        public void open(Configuration config) {
            // obtain key-value state for prediction model
            ValueStateDescriptor<Double> descriptor =
                    new ValueStateDescriptor<>(
                            // state name
                            EMA_STATE_NAME,
                            // type information of state
                            BasicTypeInfo.DOUBLE_TYPE_INFO
                    );
            emaState = getRuntimeContext().getState(descriptor);
        }
    }

    //==========================================================================
    //==== TEST LOGIC

    @Before
    public void setup() throws Exception {
        buildDataflowAndLaunchFlink(
                PRICES,
                new JsonSchema<>(String.class, SharePriceInfo.class),
                new TimestampExtractor<>(),
                PRICE_EMA,
                new JsonSchema<>(String.class, Double.class));
    }

    @Test
    public void testExponentialMovingAverage() throws Exception {
        sendValues(Lists.newArrayList(
                priceInfo(1),
                priceInfo(2),
                priceInfo(3),
                priceInfo(4),
                priceInfo(3),
                priceInfo(2),
                priceInfo(1)
        ));

        List<Double> emas = recordsConsumedOnTopic(PRICE_EMA, 7)
                .stream()
                .map(kv -> kv.f1)
                .collect(Collectors.toList());
        LOG.info("Result: {}", emas);

        Assertions.assertThat(emas).containsExactlyInAnyOrder(
                1.0,
                1.1,
                1.29,
                1.561,
                1.7049,
                1.73441,
                1.6609690000000001
        );
    }

    private Tuple2<String, SharePriceInfo> priceInfo(double value) {
        return Tuple2.of("", SharePriceInfo.make(value, 1d));
    }

    //==========================================================================
    //==== GIVEN METHODS

    private static final double EMA_ALPHA = 0.1d;
    private static final String EMA_STATE_NAME = "ema-store";
    private static final String EMA_STATE_KEY = "EMA";

    private static Double computeEma(Double currentEma, SharePriceInfo priceInfo) {

        if (currentEma==null) {
            currentEma = priceInfo.getCoins();
        }
        else {
            currentEma = priceInfo.getCoins() * EMA_ALPHA
                    + currentEma * (1 - EMA_ALPHA);
        }
        return currentEma;
    }
}
