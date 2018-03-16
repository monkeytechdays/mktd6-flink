package mktd6.flink.exchange.priceinfo;

import mktd6.model.market.SharePriceMult;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SharePriceMultSource implements SourceFunction<SharePriceMult> {

    private static final Logger LOG = LoggerFactory.getLogger(SharePriceMultSource.class);

    private transient ScheduledExecutorService executor;

    /*
     * Slightly biased increasing log-normal, very close to 1 though.
     * The share prices will increase over long periods.
     */
    private transient LogNormalDistribution logNorm;

    @Override
    public void run(SourceContext<SharePriceMult> sourceContext) throws Exception {

        logNorm = new LogNormalDistribution(0.0001, 0.01);
        executor = Executors.newScheduledThreadPool(1);

        executor.scheduleAtFixedRate(
                () -> {
                    long time = System.currentTimeMillis();
                    sourceContext.collectWithTimestamp(getMult(logNorm), time);
                    // Watermark 1 second
                    sourceContext.emitWatermark(new Watermark(time - Time.of(1, TimeUnit.SECONDS).toMilliseconds()));
                },
                0,
                1,
                TimeUnit.SECONDS
        );

        while(true) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.error("Tasks interrupted", e);
        }
        finally {
            if (!executor.isTerminated()) {
                LOG.error("Cancel non-finished tasks");
            }
            executor.shutdownNow();
            LOG.info("Shutdown finished");
        }

    }

    private static SharePriceMult getMult(LogNormalDistribution logNorm) {
        SharePriceMult mult = SharePriceMult.make(logNorm.sample());
        LOG.info("Mult: {}", mult);
        return mult;
    }
}
