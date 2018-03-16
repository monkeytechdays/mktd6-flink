package mktd6.flink.katas;

import mktd6.TimestampExtractor;
import mktd6.flink.katas.testutils.EmbeddedClustersBoilerplate;
import mktd6.model.gibber.Gibb;
import mktd6.serde.flink.JsonSchema;
import mktd6.serde.kafka.JsonSerde;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Here, we do more things with our streams, but we keep things simple with
 * a linear dataflow.
 *
 * <pre>
 *     ---o----o----o----o----
 * </pre>
 *
 * <h3>What are these <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.4/concepts/programming-model.html#programs-and-dataflows">dataflows</a> anyway?</h3>
 *
 * <p>A Dataflow, in Flink jargon, is a <b>directed acyclic graph (DAG)</b>
 * of operations distributed to a set of nodes in the cluster, each doing a
 * specific job.
 *
 * <p>The Kafka Streams Processor API allows to declare topologies in great detail,
 * but we will stick to the StreamBuilder API which provides a high-level DSL
 * doing most of the work for us. This way, we can focus on the logic internal
 * to each processing step, and leave even more boilerplate to Kafka Streams.
 */
public class Chapter01_LinearStatelessAnalysis extends EmbeddedClustersBoilerplate<String, Gibb, String> {

    //==========================================================================
    //==== ASSETS

    enum Sentiment { POS, NEG, NEUTRAL }

    enum PriceInfluence { UP, DOWN }

    private static final TopicDef<String, Gibb> GIBB_TOPIC =
            new TopicDef<>("gibb-topic",
                    new JsonSerde.StringSerde(),
                    new JsonSerde.GibbSerde());

    private static final TopicDef<String, String> PRICE_INFLUENCE_TOPIC =
            new TopicDef<>("price-influence-topic",
                    new JsonSerde.StringSerde(),
                    new JsonSerde.StringSerde());

    public Chapter01_LinearStatelessAnalysis() {
        super(GIBB_TOPIC, PRICE_INFLUENCE_TOPIC);
    }

    //==========================================================================
    //==== FIXME FLINK DATAFLOW

    /**
     * Monkeys talk to each other, and influence each other. These conversations
     * can build up confidence in the economy, or negatively impact it.
     *
     * <p>Here we wish to analyse the content of some texts sent on the Gibber
     * social network and harvested into a kafka topic. Our source is this topic.
     *
     * <p>This analysis will allow to influence the share value price: the more
     * monkeys send positive comments on the Gibber topic, the more the price
     * of shares will go up.
     *
     * <p>In order to do this analysis, we need to:
     * <ul>
     *   <li>read gibbs from the input gibb topic using
     *     (this is already provided in the source parameter)
     *
     *   <li>filter only the texts involving the #mktd6 and #bananacoins hashtags
     *     (using {@link DataStream#filter(FilterFunction)})
     *
     *   <li>detect the text sentiment
     *     (using {@link DataStream#map(MapFunction)}
     *     and the given method {@link #gibbAnalysis(Gibb)})
     *
     *   <li>filter out the texts with neutral sentiment
     *     (using {@link DataStream#filter(FilterFunction)})
     *
     *   <li>split on the number of influencing characters
     *     (using {@link DataStream#flatMap(FlatMapFunction)} and
     *     the given method {@link #influencingChars(Sentiment, String)})
     *     <br>For instance, the text "#mktd6 #bananacoins are good!!!"
     *     has 3 influencing characters '!'
     *
     *   <li>return the resulting stream events of share price updates
     *     (UP or DOWN).
     * </ul>
     *
     * <p>The resulting dataflow would look like this (read top to bottom):</p>
     * <pre>
     *     o source
     *     |
     *     o filter
     *     |
     *     o map
     *     |
     *     o filter
     *     |
     *     o flatMap
     *     |
     *     o sink
     * </pre>
     *
     */
    @Override
    protected DataStream<Tuple2<String, String>> buildFlinkDataflow(DataStream<Tuple2<String, Gibb>> source) {

        // >>> Your job starts here.

        // ######## Read the javadoc !!! ########

        // Summary:
        // filter on #mktd6 and #bananacoins
        // map to convert to sentiments
        // filter to eliminate useless sentiments
        // flatMap to convert to influencing characters

        return null;
    }

    //==========================================================================
    //==== GIVEN METHODS

    private static Tuple2<Sentiment, String> gibbAnalysis(Gibb gibb) {
        String text = gibb.getText();
        Sentiment sentiment
                = text.matches(".*\\b(smile|happy|good|yes)\\b.*") ? Sentiment.POS
                : text.matches(".*\\b(frown|sad|bad|no)\\b.*") ? Sentiment.NEG
                : Sentiment.NEUTRAL;
        return Tuple2.of(sentiment, text);
    }

    private static Stream<PriceInfluence> influencingChars(Sentiment s, String text) {
        return text.chars()
                .filter(c -> c == '!')
                .mapToObj(c -> sentimentToInfluence(s));
    }

    private static PriceInfluence sentimentToInfluence(Sentiment s) {
        return s == Sentiment.POS ? PriceInfluence.UP : PriceInfluence.DOWN;
    }

    //==========================================================================
    //==== TEST LOGIC

    @Before
    public void setup() throws Exception {
        buildDataflowAndLaunchFlink(
                GIBB_TOPIC,
                new JsonSchema<>(String.class, Gibb.class),
                new TimestampExtractor<>(),
                PRICE_INFLUENCE_TOPIC,
                new JsonSchema<>(String.class, String.class));
    }

    @Test
    public void testUpOrDown() throws Exception {
        sendValues(Arrays.asList(
                Tuple2.of("gibb", new Gibb("001", DateTime.now(DateTimeZone.UTC), "#mktd6 this is ignored")),
                Tuple2.of("gibb", new Gibb("002", DateTime.now(DateTimeZone.UTC), "#mktd6 #bananacoins are good!!!")),
                Tuple2.of("gibb", new Gibb("003", DateTime.now(DateTimeZone.UTC), "#mktd6 #bananacoins make me sad!!")),
                Tuple2.of("gibb", new Gibb("004", DateTime.now(DateTimeZone.UTC), "smile happy good !!! (ignored)")),
                Tuple2.of("gibb", new Gibb("005", DateTime.now(DateTimeZone.UTC), "#mktd6 smile! #bananacoins"))
        ));

        assertValuesReceivedOnTopic(PRICE_INFLUENCE_TOPIC, Arrays.asList(
                "UP", "UP", "UP",
                "DOWN", "DOWN",
                "UP"
        ));
    }
}
