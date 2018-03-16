package mktd6.flink.katas;

import mktd6.TimestampExtractor;
import mktd6.flink.katas.testutils.EmbeddedClustersBoilerplate;
import mktd6.model.trader.ops.Investment;
import mktd6.serde.flink.JsonSchema;
import mktd6.serde.kafka.JsonSerde;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * In this chapter, we will aggregate data which are temporally close
 * to each other.
 *
 * <h3>On the importance of keys</h3>
 *
 * <p>Flink provides {@link DataStream#keyBy(int...)} in order to
 * assigned elements of the stream to distributed operators,
 * this should not be mistaken with Kafka's concept of key/values because
 * Flink does not use Kafka for internal communication.</p>
 *
 * <p>The key of the key elements determine which operator the element is sent to.</p>
 *
 * <p><u>Conclusion:</u></p>
 * <p>Messages can only be grouped if they are on the same keyed operator, so
 * we must make sure to give the same key to all the messages we want to group
 * together.</p>
 *
 * <p>In this chapter, we will get a stream with keys already well-structured
 * keys. We only care about grouping messages together.</p>
 *
 * <p>In order to work with grouped values we will use {@link org.apache.flink.streaming.api.windowing.windows.Window}s.</p>
 *
 * <p>For more information windows, read this
 * <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/stream/operators/windows.html"></a>article</a>.
 * </p>
 */
public class Chapter03_Windowing extends EmbeddedClustersBoilerplate<String, Investment, Investment> {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter03_Windowing.class);

    private static final int TIME_WINDOW_MILLIS = 100;

    //==========================================================================
    //==== ASSETS

    private static final TopicDef<String, Investment> INVESTMENTS = new TopicDef<>(
            "investments",
            new JsonSerde.StringSerde(),
            new JsonSerde.InvestmentSerde()
    );

    private static final TopicDef<String, Investment> GROUPED_INVESTMENTS = new TopicDef<>(
            "grouped-investments",
            new JsonSerde.StringSerde(),
            new JsonSerde.InvestmentSerde()
    );

    public Chapter03_Windowing() {
        super(INVESTMENTS, GROUPED_INVESTMENTS);
    }

    //==========================================================================
    //==== FIXME FLINK DATAFLOW

    /**
     * The input topic sends investment orders from players.
     *
     * <p>Keys are the player names, and values are the investment orders.
     *
     * <p>The logic behind this exercise is that players may send a lot
     * of investment orders, and if they are close in time, we wish
     * to group them together in order to lower the load on the servers
     * which will manage them.</p>
     *
     * <p>{@link Investment}s are easy to combine: keep the transaction Id and just sul the amount invested.</p>
     *
     * <p>The relevant methods would be these:</p>
     * <ul>
     *     <li>{@link DataStream#keyBy(int...)}, returning a {@link org.apache.flink.streaming.api.datastream.KeyedStream}</li>
     *     <li>{@link org.apache.flink.streaming.api.datastream.KeyedStream#timeWindow(Time)}, returning a {@link org.apache.flink.streaming.api.datastream.WindowedStream}</li>
     *     <li>{@link org.apache.flink.streaming.api.datastream.WindowedStream#reduce(ReduceFunction)}, returning a {@link DataStream}</li>
     * </ul>
     * <p><small><i>
     *     Many of these methods have variants, with different parameters.
     *     It is instructive to have a look at them.
     * </i></small></p>
     *
     * <p>Note the sequence of classes:</p>
     * <ul>
     *     <li>we start with a regular {@link DataStream}</li>
     *     <li>transformed into a {@link org.apache.flink.streaming.api.datastream.KeyedStream}, which is an
     *     abstraction over DataStream</li>
     *     <li>converted into a {@link org.apache.flink.streaming.api.datastream.WindowedStream}, providing several
     *     ways to create aggregates</li>
     * </ul>
     */
    @Override
    protected DataStream<Tuple2<String, Investment>> buildFlinkDataflow(DataStream<Tuple2<String, Investment>> source) {

        // >>> Your job starts here.

        // ######## Read the javadoc !!! ########

        // Summary:

        // stream data from the INVESTMENTS topic
        // group the stream by key
        // window stream messages in TIME_WINDOW_MILLIS time windows
        // reduce the grouped events using the combineInvestments

        // <<< Your job ends here.
        return source
                .keyBy(0)
                .timeWindow(Time.milliseconds(TIME_WINDOW_MILLIS))
                .reduce(new ReduceInvestmentSum());
    }

    private static class ReduceInvestmentSum implements ReduceFunction<Tuple2<String, Investment>> {
        @Override
        public Tuple2<String, Investment> reduce(Tuple2<String, Investment> value1, Tuple2<String, Investment> value2) throws Exception {
            return Tuple2.of(value1.f0, Investment.make(
                    value1.f1.getTxnId(),
                    value1.f1.getInvested() + value2.f1.getInvested()
            ));
        }
    }

    //==========================================================================
    //==== TEST LOGIC

    @Before
    public void setup() throws Exception {
        JsonSchema<String, Investment> investmentSchema =
                new JsonSchema<>(String.class, Investment.class);
        buildDataflowAndLaunchFlink(
                INVESTMENTS,
                investmentSchema,
                new TimestampExtractor<>(),
                GROUPED_INVESTMENTS,
                investmentSchema);
    }


    private final int iterations = 20;
    private final CountDownLatch sendingLatch = new CountDownLatch(iterations);
    private final CountDownLatch receivingLatch = new CountDownLatch(1);
    private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(4);

    @Test
    public void testGroupingInvestments() throws Exception {
        // We send groups of investment orders every TIME_WINDOW_MILLIS
        exec.execute(() -> iterateSendingInvestments(TIME_WINDOW_MILLIS, () -> {
            sendInvestment("player1", Investment.make("txn01", 1));
            sendInvestment("player1", Investment.make("txn02", 2));
            sendInvestment("player2", Investment.make("txn03", 3));
            sendInvestment("player1", Investment.make("txn04", 4));
            sendInvestment("player2", Investment.make("txn05", 5));
            sendInvestment("player1", Investment.make("txn06", 6));
            sendInvestment("player3", Investment.make("txn06", 100));
        }));

        // We read the grouping topic and keep only the max values invested for each player
        // (With very high probability, we will have all events for each batch summed up
        // at least one time.)
        Map<String, Double> maxResults = exec
                .submit(this::readMaxValuesInGroupedInvestments)
                .get();

        // Wait for all async sending/reading to finish
        sendingLatch.await();
        receivingLatch.await();
        LOG.info("Max Results: {}", maxResults);

        // We should have both players and the correct grouped investments
        Assertions.assertThat(maxResults).containsKeys("player1", "player2", "player3");
        Assertions.assertThat(maxResults.get("player1")).isEqualTo(13); // 13 = 1+2+4+6
        Assertions.assertThat(maxResults.get("player2")).isEqualTo(8);  //  8 = 3+5
        Assertions.assertThat(maxResults.get("player3")).isEqualTo(100);
    }

    private Map<String, Double> readMaxValuesInGroupedInvestments() {
        Map<String, Double> maxResults = new ConcurrentHashMap<>();
        try {
            recordsConsumedOnTopic(GROUPED_INVESTMENTS, iterations * 2)
                    .forEach(kv -> {
                        if (!maxResults.containsKey(kv.f0) ||
                                maxResults.get(kv.f0) < kv.f1.getInvested()
                                ) {
                            maxResults.put(kv.f0, kv.f1.getInvested());
                        }
                    });
        }
        catch (Exception e) { e.printStackTrace(); }
        finally { receivingLatch.countDown(); }
        return maxResults;
    }

    private void iterateSendingInvestments(int timeWindowMillis, Runnable run) {
        try {
            run.run();
        }
        finally {
            sendingLatch.countDown();
            if (sendingLatch.getCount() > 0) {
                exec.schedule(
                        () -> iterateSendingInvestments(timeWindowMillis, run),
                        timeWindowMillis,
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    private void sendInvestment(String player, Investment inv) {
        sendValue(player, inv);
    }
}
