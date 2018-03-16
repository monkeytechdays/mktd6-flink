package mktd6.flink.exchange.dashboard;

import mktd6.flink.exchange.model.ServerTopics;
import mktd6.model.market.SharePriceInfo;
import mktd6.model.trader.Trader;
import mktd6.model.trader.TraderState;
import mktd6.serde.flink.JsonSchema;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class Dashboard {
    private static final Logger LOG = LoggerFactory.getLogger(Dashboard.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");
        consumerProperties.setProperty("group.id", "mktd-dashboard"+UUID.randomUUID().toString());
        consumerProperties.setProperty("auto.offset.reset", "earliest");

        Map<String, String> config = new HashMap<>();
        config.put("bulk.flush.max.actions", "1");   // flush inserts after every event
        config.put("cluster.name", "elasticsearch"); // default cluster name

        List<InetSocketAddress> transports = new ArrayList<>();
        // set default connection details
        transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

        /* Elasticsearch config trader states

        λ curl "http://localhost:9200/traders/_mapping/trader-states"
        {"traders":{"mappings":{"trader-states":{"properties":{"bailouts":{"type":"integer"},"coins":{"type":"double"},"fedMonkeys":{"type":"integer"},"shares":{"type":"integer"},"team":{"type":"string"},"time":{"type":"date","format":"strict_date_optional_time||epoch_millis"},"trader":{"type":"string","index":"not_analyzed"}}}}}}
         */
        env.addSource(new FlinkKafkaConsumer011<>(
                ServerTopics.TRADER_STATES.getTopicName(),
                new JsonSchema<Trader, TraderState>(Trader.class, TraderState.class),
                consumerProperties
        )).addSink(
                new ElasticsearchSink<>(config, transports, new TraderStateIndexer())
        );


        /* Elasticsearch config share prices

        λ curl "http://localhost:9200/share-prices/_mapping/share-prices"
        {"share-prices":{"mappings":{"share-prices":{"properties":{"coins":{"type":"double"},"time":{"type":"date","format":"strict_date_optional_time||epoch_millis"}}}}}}%
         */
        env.addSource(new FlinkKafkaConsumer011<>(
                TopicDef.SHARE_PRICE.getTopicName(),
                new JsonSchema<String, SharePriceInfo>(String.class, SharePriceInfo.class),
                consumerProperties
        ))
//                .print();
        .addSink(
                new ElasticsearchSink<>(config, transports, new SharePricesIndexer())
        );

        env.execute("Dashboard");
    }

    public static class TraderStateIndexer
            implements ElasticsearchSinkFunction<Tuple2<Trader, TraderState>> {

        // construct index request
        @Override
        public void process(
                Tuple2<Trader, TraderState> record,
                RuntimeContext ctx,
                RequestIndexer indexer) {

            // construct JSON document to index
            Map<String, String> json = new HashMap<>();
            json.put("team", record.f0.getTeam().name());
            json.put("trader", record.f0.getName());
            json.put("time", String.valueOf(record.f1.getTime().getMillis()));         // timestamp
            json.put("coins", String.valueOf(record.f1.getCoins()));
            json.put("shares", String.valueOf(record.f1.getShares()));
            json.put("bailouts", String.valueOf(record.f1.getBailouts()));
            json.put("fedMonkeys", String.valueOf(record.f1.getFedMonkeys()));

            IndexRequest rqst = Requests.indexRequest()
                    .index("traders")           // index name
                    .type("trader-states")     // mapping name
                    .source(json);

            indexer.add(rqst);
        }
    }

    public static class SharePricesIndexer
            implements ElasticsearchSinkFunction<Tuple2<String, SharePriceInfo>> {

        // construct index request
        @Override
        public void process(
                Tuple2<String, SharePriceInfo> record,
                RuntimeContext ctx,
                RequestIndexer indexer) {

            // construct JSON document to index
            Map<String, String> json = new HashMap<>();
            json.put("time", String.valueOf(record.f1.getTime().getMillis()));         // timestamp
            json.put("coins", String.valueOf(record.f1.getCoins()));

            IndexRequest rqst = Requests.indexRequest()
                    .index("share-prices")           // index name
                    .type("share-prices")     // mapping name
                    .source(json);

            indexer.add(rqst);
        }
    }
}
