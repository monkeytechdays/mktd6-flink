package mktd6.model.gibber;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import mktd6.model.Timestamped;
import org.joda.time.DateTime;

/**
 * Gibber is the twitter equivalent in the monkonomy.
 */
public class Gibb implements Timestamped {

    @JsonProperty
    private final String id;
    @JsonProperty
    private final DateTime time;
    @JsonProperty
    private final String text;

    @JsonCreator
    public Gibb(
            @JsonProperty("id") String id,
            @JsonProperty("time") DateTime time,
            @JsonProperty("text") String text
    ) {
        this.id = id;
        this.time = time;
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public String getId() {
        return id;
    }

    public DateTime getTime() {
        return time;
    }
}
