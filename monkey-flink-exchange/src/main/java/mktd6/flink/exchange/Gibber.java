package mktd6.flink.exchange;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import mktd6.model.gibber.Gibb;
import mktd6.serde.flink.JsonSchema;
import mktd6.topic.TopicDef;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;
import java.util.regex.Pattern;

public class Gibber {
    private static final Logger LOG = LoggerFactory.getLogger(Gibber.class);

    private static final String BOOTSTRAP_SERVERS =
            System.getenv("BOOTSTRAP_SERVERS");
    private static final String TWITTER_CONSUMER_KEY =
            System.getenv("TWITTER_CONSUMER_KEY");
    private static final String TWITTER_CONSUMER_SECRET =
            System.getenv("TWITTER_CONSUMER_SECRET");
    private static final String TWITTER_ACCESS_TOKEN =
            System.getenv("TWITTER_ACCESS_TOKEN");
    private static final String TWITTER_ACCESS_TOKEN_SECRET =
            System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: " + Gibber.class.getSimpleName() +
                "[--bootstrap-servers <bootstrap-servers> --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]");

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(params.getInt("parallelism", 1));

        // get input data
        Properties twitterProps;
        String bootstrapServers;
        if (params.has(TwitterSource.CONSUMER_KEY)
                && params.has(TwitterSource.CONSUMER_SECRET)
                && params.has(TwitterSource.TOKEN)
                && params.has(TwitterSource.TOKEN_SECRET)
                && params.has("bootstrap-servers")
                ) {
            twitterProps = params.getProperties();
            bootstrapServers = params.get("bootstrap-servers");
        } else {
            twitterProps = new Properties();
            twitterProps.setProperty(TwitterSource.CONSUMER_KEY, TWITTER_CONSUMER_KEY);
            twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, TWITTER_CONSUMER_SECRET);
            twitterProps.setProperty(TwitterSource.TOKEN, TWITTER_ACCESS_TOKEN);
            twitterProps.setProperty(TwitterSource.TOKEN_SECRET, TWITTER_ACCESS_TOKEN_SECRET);
            bootstrapServers = Strings.isNullOrEmpty(BOOTSTRAP_SERVERS) ? "localhost:9092" : BOOTSTRAP_SERVERS;
        }

        TwitterSource twitterSource = new TwitterSource(twitterProps);
        twitterSource.setCustomEndpointInitializer(new TwitterEndpointInitializer());
        DataStream<String> twitterStream = env.addSource(twitterSource);

        DataStream<Tuple3<String, DateTime, String>> banana = twitterStream
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value;
                    }
                })
                .flatMap(new FilterAndMapBananaGibb());

        banana.map(new MapGibbIdentity())
                .addSink(new FlinkKafkaProducer011<>(
                        bootstrapServers,
                        TopicDef.GIBBS.getTopicName(),
                        new JsonSchema<>(String.class, Gibb.class)
                ));

        // Commented: too much hype
//        banana.map(new MapGibbMonkey())
//                .addSink(new FlinkKafkaProducer011<>(
//                        bootstrapServers,
//                        TopicDef.GIBBS.getTopicName(),
//                        new JsonSchema<>(String.class, Gibb.class)
//                ));

        // execute program
        env.execute(Gibber.class.getSimpleName());
    }

    private static final Pattern banana = Pattern.compile("(?i).*banana.*");

    /**
     * Deserialize JSON from twitter source and filter on banana pattern matcher
     */
    public static class FilterAndMapBananaGibb implements FlatMapFunction<String, Tuple3<String, DateTime, String>> {

        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String tweet, Collector<Tuple3<String, DateTime, String>> gibber) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(tweet, JsonNode.class);
            boolean hasId = jsonNode.has("id_str");
            boolean hasText = jsonNode.has("text");
            if (hasId
                    && hasText
                    && banana.matcher(jsonNode.get("text").asText()).matches()) {
                gibber.collect(Tuple3.of(
                        jsonNode.get("id_str").asText(),
                        DateTime.now(DateTimeZone.UTC),
                        jsonNode.get("text").asText()
                ));
            }
        }
    }

    private static class MapGibbIdentity implements MapFunction<Tuple3<String,DateTime,String>, Tuple2<String, Gibb>> {
        @Override
        public Tuple2<String, Gibb> map(Tuple3<String, DateTime, String> value) throws Exception {
            return Tuple2.of("FOO", new Gibb(value.f0, value.f1, value.f2));
        }
    }

    private static class MapGibbMonkey implements MapFunction<Tuple3<String,DateTime,String>, Tuple2<String, Gibb>> {
        @Override
        public Tuple2<String, Gibb> map(Tuple3<String, DateTime, String> value) throws Exception {
            return Tuple2.of(
                    "FOO",
                    new Gibb(
                            value.f0+"xxx",
                            value.f1.plus(1L),
                            "very good banana down my throat, i'm happy and love up up up"
                    )
            );
        }
    }

    private static class TwitterEndpointInitializer implements TwitterSource.EndpointInitializer, Serializable {
        @Override
        public StreamingEndpoint createEndpoint() {
            return new StatusesFilterEndpoint()
                    .trackTerms(Lists.newArrayList("kafka", "banana"));
        }
    }
}
