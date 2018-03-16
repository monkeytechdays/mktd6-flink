package mktd6.flink.katas.testutils;

import mktd6.topic.TopicDef;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBase;
import org.apache.flink.streaming.connectors.kafka.testutils.JobManagerCommunicationUtils;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.util.*;
import java.util.concurrent.*;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public abstract class EmbeddedClustersBoilerplate<K, I, O> extends KafkaTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedClustersBoilerplate.class);

    static {
        timeout = new FiniteDuration(30L, TimeUnit.SECONDS);
    }
    private TopicDef<K, I> inputTopic;
    private TopicDef<K, O> outputTopic;

    public EmbeddedClustersBoilerplate(TopicDef<K, I> inputTopic, TopicDef<K, O> outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic= outputTopic;
    }

    private static ScheduledExecutorService executorService;
    private CountDownLatch latch;
    private Exception dataflowException;
    private KafkaProducer<K, I> producer;

    protected void createTopic(TopicDef<?,?> topicDef) throws InterruptedException {
        createTestTopic(topicDef.getTopicName(), 1, 1);
    }

    protected void deleteTopic(TopicDef<?,?> topicDef) throws InterruptedException {
        deleteTestTopic(topicDef.getTopicName());
    }

    @BeforeClass
    public static void start() {
        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    @Before
    public void prepareBoilerplate() throws Exception {
        JobManagerCommunicationUtils.waitUntilNoJobIsRunning(flink.getLeaderGateway(timeout));
        createTestTopic(inputTopic.getTopicName(), 1, 1);
        createTestTopic(outputTopic.getTopicName(), 1, 1);
        Properties producerConfig = new Properties();
        producerConfig.putAll(standardProps);
        producerConfig.putAll(secureProps);
        producerConfig.put("key.serializer", inputTopic.getKeySerializerClass().getName());
        producerConfig.put("value.serializer", inputTopic.getValueSerializerClass().getName());
        producer = new KafkaProducer<>(producerConfig);
    }

    @After
    public void cleanBoilerplate() {
        deleteTestTopic(inputTopic.getTopicName());
        deleteTestTopic(outputTopic.getTopicName());
        for (KafkaProducer producer : cachedProducers.values()) {
            if(producer != null) {
                producer.close();
            }
        }
        if(producer != null) {
            producer.close();
        }
    }

    @AfterClass
    public static void stop() {
        try {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.error("Tasks interrupted", e);
        }
        finally {
            if (!executorService.isTerminated()) {
                LOG.error("Cancel non-finished tasks");
            }
            executorService.shutdownNow();
            LOG.info("Shutdown finished");
        }
    }

    protected void buildDataflowAndLaunchFlink(TopicDef<K, I> inputTopicDef,
                                               KeyedDeserializationSchema<Tuple2<K, I>> consumedDeserializer,
                                               AssignerWithPeriodicWatermarks<Tuple2<K, I>> timestampExctractor,
                                               TopicDef<K, O> outputTopicDef,
                                               KeyedSerializationSchema<Tuple2<K, O>> producedSerializer)
    {
        latch = new CountDownLatch(1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().disableSysoutLogging();

        Properties props = new Properties();
        props.putAll(standardProps);
        props.putAll(secureProps);

        StreamExecutionEnvironment finalEnv = env;
        executorService.submit(() -> {
            String threadName = Thread.currentThread().getName();
            try {
                FlinkKafkaConsumer011<Tuple2<K, I>> consumer = new FlinkKafkaConsumer011<>(
                        inputTopicDef.getTopicName(),
                        consumedDeserializer,
                        props
                );
                consumer.assignTimestampsAndWatermarks(timestampExctractor);

                DataStreamSource<Tuple2<K, I>> source = finalEnv.addSource(consumer);

                buildFlinkDataflow(source)
                        .addSink(new FlinkKafkaProducer011<>(
                                brokerConnectionStrings,
                                outputTopicDef.getTopicName(),
                                producedSerializer
                        ));

                LOG.info("-------------------------------------------------------------------------\n    Starting DATAFLOW\n-------------------------------------------------------------------------");
                finalEnv.execute(String.format(
                        "Execute Flink dataflow in thread '%s'", threadName));
            } catch (Exception e) {
                dataflowException = e;
                LOG.error("-------------------------------------------------------------------------\n    DATAFLOW FAILED!\n-------------------------------------------------------------------------", e);
                latch.countDown();
            }
        });
    }

    protected abstract DataStream<Tuple2<K, O>> buildFlinkDataflow(DataStream<Tuple2<K, I>> source);

    Map<String, KafkaProducer> cachedProducers = new HashMap<>();

    protected void sendValue(K key, I value) {
        KafkaProducer<K, I> producer;
        if (cachedProducers.containsKey(inputTopic.getTopicName())) {
            producer = cachedProducers.get(inputTopic.getTopicName());
        }
        else {
            Properties producerConfig = new Properties();
            producerConfig.putAll(standardProps);
            producerConfig.putAll(secureProps);
            producerConfig.put("key.serializer", inputTopic.getKeySerializerClass().getName());
            producerConfig.put("value.serializer", inputTopic.getValueSerializerClass().getName());
            producer = new KafkaProducer<>(producerConfig);
            cachedProducers.put(inputTopic.getTopicName(), producer);
        }

        ArrayList<Tuple2<K, I>> keyedRecords = Lists.newArrayList(Tuple2.of(key, value));

        try {
            produceKeyValuesSynchronously(producer, inputTopic.getTopicName(), keyedRecords);
        } catch (Exception e) {
            if (producer!=null) {
                producer.close();
                cachedProducers.remove(inputTopic.getTopicName());
            }
        }
    }

    protected void sendValues(List<Tuple2<K, I>> values) throws Exception {
        ArrayList<Tuple2<K, I>> keyedRecords = Lists.newArrayList();

        for (Tuple2<K, I> kv : values) {
            keyedRecords.add(kv);
        }

        produceKeyValuesSynchronously(producer, inputTopic.getTopicName(), keyedRecords);
    }

    private <K, V> void produceKeyValuesSynchronously(KafkaProducer<K, V> producer, String topic, Collection<Tuple2<K, V>> records) throws InterruptedException, ExecutionException {
        for (Tuple2<K, V> record : records) {
            Future f = producer.send(new ProducerRecord<>(topic, record.f0, record.f1));
            f.get();
        }

        producer.flush();
    }

    protected <K, V> void assertValuesReceivedOnTopic(TopicDef<K, V> topicDef, List<V> expected) throws Exception {
        List<V> actual = waitUntilMinKeyValueRecordsReceived(topicDef.getTopicName(),
                topicDef.getKeyDeserializerClass(),
                topicDef.getValueDeserializerClass(),
                expected.size()
        ).stream()
                .map(t -> t.f1)
                .collect(toList());

        assertThat(actual, equalTo(expected));
    }

    private <K, V> List<Tuple2<K, V>> waitUntilMinKeyValueRecordsReceived(
            String topic,
            Class<? extends Deserializer<K>> keyDeserializer,
            Class<? extends Deserializer<V>> valueDeserializer,
            int number
    ) throws Exception {
        long timeout = 120000L;
        long startMillis = System.currentTimeMillis();
        List<Tuple2<K, V>> actual = Lists.newArrayList();
        Properties consumerConfig = new Properties();
        consumerConfig.putAll(standardProps);
        consumerConfig.putAll(secureProps);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        while(System.currentTimeMillis() < startMillis + timeout) {
            stopFailedDataflow();
            Collection<ConsumerRecord<K, V>> records = kafkaServer.getAllRecordsFromTopic(consumerConfig, topic, 0, 1000L);

            for (ConsumerRecord<K, V> record : records) {
                stopFailedDataflow();
                actual.add(Tuple2.of(record.key(), record.value()));
            }

            if(actual.size() >= number) {
                break;
            }
        }

        if (actual.size() < number) {
            fail(String.format("Got %s elements out of %s expected after waiting for %sms. Failing the job: timeout.", actual.size(), number, timeout));
        }

        return actual;
    }

    private void stopFailedDataflow() {
        if (latch.getCount()==0) {
            LOG.error("DATAFLOW FAILED", dataflowException);
            fail("DATAFLOW FAILED");
        }
    }

    protected <K, V> List<Tuple2<K,V>> recordsConsumedOnTopic(TopicDef<K, V> topicDef, int number) throws Exception {
        return waitUntilMinKeyValueRecordsReceived(topicDef.getTopicName(),
                topicDef.getKeyDeserializerClass(),
                topicDef.getValueDeserializerClass(),
                number);
    }
}
