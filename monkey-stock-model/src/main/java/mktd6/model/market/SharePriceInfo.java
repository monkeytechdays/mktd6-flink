package mktd6.model.market;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import mktd6.model.Timestamped;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class SharePriceInfo implements Timestamped {

    @JsonProperty
    private final DateTime time;
    @JsonProperty
    private final double coins;
    @JsonProperty
    private final SharePriceSimpleForecast forecast;

    @JsonCreator
    public SharePriceInfo(
            @JsonProperty("time") DateTime time,
            @JsonProperty("coins") double coins,
            @JsonProperty("forecast") SharePriceSimpleForecast forecast
    ) {
        this.time = time;
        this.coins = coins;
        this.forecast = forecast;
    }

    public static SharePriceInfo make(double coins, double mult) {
        return new SharePriceInfo(
            DateTime.now(DateTimeZone.UTC),
            coins,
            new SharePriceSimpleForecast(mult)
        );
    }

    public double getCoins() {
        return coins;
    }

    public DateTime getTime() {
        return time;
    }

    public SharePriceSimpleForecast getForecast() {
        return forecast;
    }
}
