package mktd6.flink.katas;

import mktd6.flink.katas.testutils.EmbeddedClustersBoilerplate;
import mktd6.model.trader.ops.MarketOrder;
import mktd6.model.trader.ops.MarketOrderType;
import mktd6.serde.flink.JsonSchema;
import mktd6.serde.kafka.JsonSerde;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.types.Either;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * Linear transformations are useful, but being able to "split"
 * stream transformations is even more powerful.
 *
 * <p>
 * In this chapter, we use three DataStream methods:
 *   <ul>
 *   <li>{@link DataStream#split(OutputSelector)}</li>
 *   <li>{@link SplitStream#select(String...)}</li>
 *   <li>{@link DataStream#union(DataStream[])}</li>
 *   </ul>
 * </p>
 *
 * <h3>Splitting by applying several transformations to the same stream</h3>
 *
 * <p>One may split a dataflow by saving a DataStream instance and apply
 * several transformations to it. For instance, you may choose to do this:</p>
 *
 * <pre>
 *              print
 *          .---o
 *          |
 *      .---o--------o-------
 *      |   filter   map
 * -----o
 *      |
 *      .---o----------------
 *          map
 * </pre>
 *
 * <h3>Merging streams having the same key and value types</h3>
 *
 * <p>Once streams are split, they even can be merged back together as long as
 * their key/value types are the same.
 *
 */
public class Chapter02_SplitSelectUnion extends EmbeddedClustersBoilerplate<String, String, MarketOrder> {

    private static final Logger LOG = LoggerFactory.getLogger(Chapter02_SplitSelectUnion.class);

    //==========================================================================
    //==== ASSETS

    private static final TopicDef<String, String> ORDER_TEXT =
            new TopicDef<>(
                    "order-texts",
                    new JsonSerde.StringSerde(),
                    new JsonSerde.StringSerde());

    private static final TopicDef<String, MarketOrder> VALID_ORDERS =
            new TopicDef<>(
                    "valid-orders",
                    new JsonSerde.StringSerde(),
                    new JsonSerde.MarketOrderSerde());

    private static final TopicDef<String, String> INVALID_ORDERS =
            new TopicDef<>(
                    "invalid-orders",
                    new JsonSerde.StringSerde(),
                    new JsonSerde.StringSerde());

    public Chapter02_SplitSelectUnion() {
        super(ORDER_TEXT, VALID_ORDERS);
    }

    //==========================================================================
    //==== FIXME FLINK DATAFLOW

    /**
     * Traders can send {@link MarketOrder}s of two kinds:
     * {@link MarketOrderType#BUY} and {@link MarketOrderType#SELL}.
     *
     * <p>The {@link #ORDER_TEXT} input topic should contain null keys, and values of the form:
     * <tt>trader transactionId (BUY|SELL) 999</tt> where 999 is a valid
     * positive number.
     *
     * <p>Here, we will parse strings into market orders using the given
     * {@link #parseOrder(String)} method.</p>
     *
     * <h3>Branching</h3>
     *
     * <p>We will then branch into 3 different streams, using {@link DataStream#split(OutputSelector)}:
     * <ul>
     *     <li>BUY orders</li>
     *     <li>SELL orders</li>
     *     <li>invalid orders</li>
     * </ul>
     *
     * <p>Note that in case of invalid orders, we keep the whole text value
     * as the key.</p>
     *
     * <h3>Writing invalid orders to the {@link #INVALID_ORDERS} topic</h3>
     *
     * <p>We will write the invalid orders to a separate topic to be handled
     * by a specific treatment, for instance security verifications, logging,
     * etc.</p>
     *
     * <h3>Filtering both valid order streams</h3>
     *
     * <p>For both the BUY and SELL streams, we apply the
     * {@link #tooManySharesInOrder(Tuple2)} filter.</p>
     *
     * <p><small><i>Obviously,
     * this could be done in a single stream, but for the sake of the
     * exercise, we'll do it on both. Feel free to modify the dataflow
     * at will after you have done it this way.</i></small></p>
     *
     * <h3>Merge the valid order streams and output to {@link #VALID_ORDERS} topic</h3>
     *
     * <p>The topology we aim for looks like this:</pre>
     *
     * <pre>
     *                       filter
     *            .->-(BUY)--o-------.
     *     branch |          filter  | merge  to
     * o-->-------o->-(SELL)-o-------o--------o
     * stream     |
     *            .->-(INVALID)--o to
     * </pre>
     */
    @Override
    protected DataStream<Tuple2<String, MarketOrder>> buildFlinkDataflow(DataStream<Tuple2<String, String>> source) {

        // >>> Your job starts here.

        // ######## Read the javadoc !!! ########

        // Summary:
        // map using the parseOrder method
        // branch using 3 predicates

        // map the invalid orders back into to INVALID_ORDERS topic

        // union back the BUY and SELL orders
        // filter them using tooManySharesInOrder
        // send to the VALID_ORDERS topic

        // <<< Your job ends here.
        SplitStream<Either<String, Tuple2<String, MarketOrder>>> branches = source
                .map(new ParseOrderOrInvalid())
                .split(new OrderSelector());

        branches.select("invalid")
                .map(new MapInvalidToString())
                .addSink(new FlinkKafkaProducer011<>(
                        brokerConnectionStrings,
                        INVALID_ORDERS.getTopicName(),
                        new JsonSchema<>(String.class, String.class)
                ));

        return branches.select("buy")
                .map(new MapValidToString())
                .filter(order -> !tooManySharesInOrder(order))
                .union(branches.select("sell")
                        .map(new MapValidToString()))
                ;
    }

    private static class ParseOrderOrInvalid implements MapFunction<Tuple2<String,String>, Either<String, Tuple2<String, MarketOrder>>> {
        @Override
        public Either<String, Tuple2<String, MarketOrder>> map(Tuple2<String, String> value) throws Exception {
            return parseOrder(value.f1);
        }
    }

    private static class OrderSelector implements OutputSelector<Either<String, Tuple2<String, MarketOrder>>> {
        @Override
        public Iterable<String> select(Either<String, Tuple2<String, MarketOrder>> value) {
            List<String> outputNames = new ArrayList<>();
            if (value.isLeft()) {
                outputNames.add("invalid");
            } else {
                MarketOrder order = value.right().f1;
                if (order.getType() == MarketOrderType.BUY) {
                    outputNames.add("buy");
                } else if (order.getType() == MarketOrderType.SELL) {
                    outputNames.add("sell");
                }
            }
            return outputNames;
        }
    }

    private static class MapInvalidToString implements MapFunction<Either<String,Tuple2<String,MarketOrder>>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(Either<String, Tuple2<String, MarketOrder>> value) throws Exception {
            return Tuple2.of("", value.left());
        }
    }

    private static class MapValidToString implements MapFunction<Either<String,Tuple2<String,MarketOrder>>, Tuple2<String, MarketOrder>> {
        @Override
        public Tuple2<String, MarketOrder> map(Either<String, Tuple2<String, MarketOrder>> value) throws Exception {
            return Tuple2.of(value.right().f0, value.right().f1);
        }
    }


    //==========================================================================
    //==== GIVEN METHODS

    private static final Pattern PATTERN = Pattern.compile("(?i)^(?<time>[a-z0-9]+) (?<player>[a-z0-9]+) (?<id>[a-z0-9]+) (?<type>BUY|SELL) (?<shares>[0-9]+)$");

    private static Either<String, Tuple2<String, MarketOrder>> parseOrder(String input) {
        try {
            Matcher m = PATTERN.matcher(input);
            if (m.matches()) {
                Long time = Long.parseLong(m.group("time"));
                String id = m.group("id");
                MarketOrderType type = MarketOrderType.valueOf(m.group("type"));
                int shares = Integer.parseInt(m.group("shares"), 10);
                return Either.Right(Tuple2.of(
                        m.group("player"),
                        new MarketOrder(new DateTime(time, DateTimeZone.UTC), id, type, shares)
                ));
            }
            else {
                return Either.Left(input);
            }
        }
        catch(Exception e) {
            LOG.error(e.getMessage(), e);
            return Either.Left(input);
        }
    }

    /**
     * We don't want Kerviel level catastrophes anymore.
     * Orders too big are forbidden.
     * Use this method to keep only the orders buying/selling
     * less than 1000 shares.
     */
    private static boolean tooManySharesInOrder(Tuple2<String, MarketOrder> marketOrder) {
        return marketOrder.f1.getShares() > 1000;
    }

    //==========================================================================
    //==== TEST LOGIC

    @Before
    public void setup() throws Exception {
        createTopic(INVALID_ORDERS);
        buildDataflowAndLaunchFlink(
                ORDER_TEXT,
                new JsonSchema<>(String.class, String.class),
                new MarketOrderTSExtractor(),
                VALID_ORDERS,
                new JsonSchema<>(String.class, MarketOrder.class));
    }

    @After
    public void teardown() throws InterruptedException {
        deleteTopic(INVALID_ORDERS);
    }

    @Test
    public void testValidInvalid() throws Exception {

        long now = DateTime.now(DateTimeZone.UTC).getMillis();
        String ORDER_1 = String.format("%d player1 txn01 BUY 5", ++now);
        String ORDER_2 = String.format("%d player2 txn02 BUY 99999", ++now);
        String ORDER_3 = String.format("%d player2 txn03 SELL 8", ++now);
        String ORDER_4 = String.format("%d player3 txn04 INVALID ORDER", ++now);
        String ORDER_5 = String.format("%d player1 txn01 SELL 5", ++now);

        sendValues(Arrays.asList(
                Tuple2.of("", ORDER_1),
                Tuple2.of("", ORDER_2),
                Tuple2.of("", ORDER_3),
                Tuple2.of("", ORDER_4),
                Tuple2.of("", ORDER_5)
        ));

        List<String> records = recordsConsumedOnTopic(VALID_ORDERS, 3)
                .stream()
                .map(kv -> String.format("%s %s %s %s %d",
                        kv.f1.getTime().getMillis(),
                        kv.f0,
                        kv.f1.getTxnId(),
                        kv.f1.getType().name(),
                        kv.f1.getShares())
                )
                .collect(Collectors.toList());

        assertThat(records).containsExactlyInAnyOrder(
                ORDER_1,
                ORDER_3,
                ORDER_5
        );

        assertValuesReceivedOnTopic(INVALID_ORDERS, Collections.singletonList(
                ORDER_4
        ));
    }

    public static class MarketOrderTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, String>> {

        public MarketOrderTSExtractor() {
            super(Time.milliseconds(10));
        }

        @Override
        public long extractTimestamp(Tuple2<String, String> element) {
            // Notice we do not use the same matcher here, otherwise the exception
            // would be thrown when reaching the first invalid order,
            // effectively killing the dataflow
            Matcher m = Pattern.compile("(?i)^(?<time>[a-z0-9]+) .*").matcher(element.f1);
            if (m.matches()) {
                return Long.parseLong(m.group("time"));
            }
            throw new IllegalArgumentException("MarketOrder does not match expected format.");
        }
    }
}
