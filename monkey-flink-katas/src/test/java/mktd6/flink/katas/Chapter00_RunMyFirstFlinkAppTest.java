package mktd6.flink.katas;

import mktd6.TimestampExtractor;
import mktd6.flink.katas.testutils.EmbeddedClustersBoilerplate;
import mktd6.model.market.SharePriceInfo;
import mktd6.serde.flink.JsonSchema;
import mktd6.serde.kafka.JsonSerde;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;


/**
 * Here, we write and run our very first Flink app.
 *
 * <p>Most of the boilerplate is hidden away in the parent class,
 * which will be used in all chapters. You may take a look at
 * this class, or skip it entirely. Its purpose it to hide away
 * the tedious details of how to configure and run a Kafka Streams
 * app. This is not very important, but often clunky and repetitive
 * in actual code, so some of it is hidden away, and only the
 * strictly necessary boilerplate code is shown.
 *
 * <p>Your job is to complete the buildFlinkDataflow method.
 *
 * <p>In order to run our first simple Flink application,
 * we use an embedded Flink cluster as well as an
 * embedded kafka cluster. This technique can be used in
 * tests and during development, to prevent the need for an
 * actual Kafka cluster, but it is heavy on resources and should
 * not necessarily be the default choice for unit tests.
 *
 * Flink can also run as a standalone application which is
 * handy for testing purposes before being deployed with a various
 * cluster managers (Yarn, Mesos, Kubernetes, Docker, AWS, ...)
 */
public class Chapter00_RunMyFirstFlinkAppTest extends EmbeddedClustersBoilerplate<String, SharePriceInfo, String> {

    //==========================================================================
    //==== ASSETS

    // We will be reading from this topic.
    // (TopicDef is a helper class that is not part of Kafka.)
    private static final TopicDef<String, SharePriceInfo> SHARE_PRICE_TOPIC = TopicDef.SHARE_PRICE;

    // And we will be writing to this topic:
    private static final TopicDef<String, String> BUY_OR_SELL_TOPIC = new TopicDef<>(
            "buy-or-sell",
            new JsonSerde.StringSerde(),  // Keys are Strings
            new JsonSerde.StringSerde()); // Values are Strings

    // All our tests will have to implement a constructor like this one
    // in order to provide the main input and output topics.
    // Of course other kafka sources/sinks can be configured as part
    // of the dataflow if needed as you shall see later, this is just
    // handy way to not repeat boilerplate for single source/sink tests.
    public Chapter00_RunMyFirstFlinkAppTest() {
        super(SHARE_PRICE_TOPIC, BUY_OR_SELL_TOPIC);
    }

    /**
     * We want to decide whether we should buy or sell shares in the market.
     *
     * <p>We are given a stream of SharePriceInfo, containing a price forecast.
     * The forecast is a multiplicator (always a positive number):
     * <ul>
     * <li>if it is greater than 1, it means the price is likely to go up
     * <li>if it is lower than 1, it means the price is likely to go down.
     * </ul>
     *
     * <p>We will be coding how to create such a forecast in a later chapter.
     *
     * <p>For now, we want to convert each SharePriceInfo into a String
     * containing "BUY" if we think we should buy, or "SELL" if we think
     * we should sell, based only on the forecast.
     *
     * The buildFlinkDataflow provides the source stream and expects you
     * to return an output stream.
     *
     * DataStream is Flink's abstraction of a stream of infinite data.
     * Because Flink is a Stream Processing engine as well as a
     * Batch Processing engine, it provides powerful abstractions for each.
     * The focus of the day is on Stream Processing
     * See https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/datastream_api.html
     */
    @Override
    protected DataStream<Tuple2<String, String>> buildFlinkDataflow(DataStream<Tuple2<String, SharePriceInfo>> source) {
        // We read from the share price topic

        /*====
               You should do something here, using the DataStream API.
               This may help you:
               https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/stream/operators/
               https://ci.apache.org/projects/flink/flink-docs-release-1.4/api/java/

               WARNING: Flink being a Distributed processing engine, your code
                        will not be instantiated and run right away.
                        Instead Flink will serialize every operation in order to
                        distribute it on the cluster.
                        This does not work so well with lambda expressions because
                        of type inference issues.
                        Here you will have to fight against your IDE who will
                        constantly propose you to use lambdas, but I encourage
                        you to declare each and every of your operators as
                        classes of inner-classes.
                        Or you could try to play with the eclise jdt and the
                        maven configuration to make it work, but this is not
                        really the topic of the day.
            ====*/

        return source.map(new BuyOrSell());
    }

    private static class BuyOrSell implements MapFunction<Tuple2<String, SharePriceInfo>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(Tuple2<String, SharePriceInfo> price) throws Exception {
            return Tuple2.of("",
                    price.f1.getForecast().getMult() > 1 ? "BUY" : "SELL");
        }
    }

    //==========================================================================
    //==== TEST LOGIC

    // Now for some tedious ceremony for the tests.
    // Look at it once, and then ignore it in future chapters.
    @Before
    public void setup() throws Exception {
        // Setup and execute the dataflow after the embedded clusters have started
        // Provide the topic and serializers for the source and sink topics
        // as well as a timestamp extractor use to retrieve the event time of the data
        buildDataflowAndLaunchFlink(
                SHARE_PRICE_TOPIC,
                new JsonSchema<>(String.class, SharePriceInfo.class),
                new TimestampExtractor<>(),
                BUY_OR_SELL_TOPIC,
                new JsonSchema<>(String.class, String.class));
    }

    @Test(timeout = 60000)
    public void testBuyOrSellLogic() throws Exception {

        // Send this list of objects in the source topic.
        // All the exercises will follow the same pattern using Tuple2
        // to materialize Kafka Keys and Values.
        // (Don't worry about keys for now, we won't use them.)
        sendValues(Arrays.asList(
                Tuple2.of("price", SharePriceInfo.make(1.0f, 3.14f)),   // mult is > 1, BUY
                Tuple2.of("price", SharePriceInfo.make(1.1f, 0.42f)),   // mult is < 1, SELL
                Tuple2.of("price", SharePriceInfo.make(0.9f, 0.9999f)), // mult is < 1, SELL
                Tuple2.of("price", SharePriceInfo.make(1.05f,1.0001f))  // mult is > 1, BUY
        ));

        // These are the values we expect to receive in the sink topic.
        // Here again, keys are ignored, we care only about values.
        assertValuesReceivedOnTopic(BUY_OR_SELL_TOPIC, Arrays.asList(
                "BUY",
                "SELL",
                "SELL",
                "BUY"
        ));
    }
}
