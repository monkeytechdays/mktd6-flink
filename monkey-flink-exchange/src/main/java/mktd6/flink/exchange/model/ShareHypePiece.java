package mktd6.flink.exchange.model;

import mktd6.model.gibber.Gibb;
import mktd6.serde.kafka.BaseJsonSerde;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The hype measures how many positive/negative talk there is
 * about the shares in Gibber. A hype piece is a contribution
 * to the hype by a single gibb.
 */
public class ShareHypePiece {

    private static final Logger LOG = LoggerFactory.getLogger(ShareHypePiece.class);

    private static final List<String> POSITIVE = Arrays.asList(
        "yes", "happy", "good", "best", "up", "safe",
        "buy", "like", "love", "high", "nice", "certain", "sure", "solid"
    );

    private static final List<String> NEGATIVE = Arrays.asList(
        "no", "sad", "bad", "worst", "down", "dangerous", "danger",
        "sell", "dislike", "hate", "low", "bubble", "burst", "risk"
    );

    private final DateTime time;
    private final String gibbId;
    private final boolean positive;
    private final String word;

    public ShareHypePiece(DateTime time, String gibbId, boolean positive, String word) {
        this.time = time;
        this.gibbId = gibbId;
        this.positive = positive;
        this.word = word;
    }

    public DateTime getTime() {
        return time;
    }

    public String getGibbId() {
        return gibbId;
    }

    public String getWord() {
        return word;
    }

    public boolean isPositive() {
        return positive;
    }

    public int getInfluence() {
        return isPositive() ? +1 : -1;
    }

    public static List<ShareHypePiece> hypePieces(Gibb gibb) {
        String clean = gibb.getText().toLowerCase()
                .replaceAll("[^a-z \n]+", "")
                .replaceAll("\\s+", " ");
        //LOG.info("Clean gibb: {}", clean);
        return Arrays.stream(clean
            .split(" "))
            .flatMap(word -> {
                if (POSITIVE.contains(word)) {
                    return Stream.of(new ShareHypePiece(gibb.getTime(), gibb.getId(), true, word));
                }
                else if (NEGATIVE.contains(word)) {
                    return Stream.of(new ShareHypePiece(gibb.getTime(), gibb.getId(), false, word));
                }
                else {
                    return Stream.empty();
                }
            })
            .collect(Collectors.toList());
    }

    public static class Serde extends BaseJsonSerde<ShareHypePiece> {
        public Serde() { super(ShareHypePiece.class); }
    }
}
