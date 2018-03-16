package mktd6.model.market;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * A simple forecast is created by the exchange supervisor like this:
 * take the price info SMA20 and divide the current price by the SMA20.
 *
 * <p>
 *     <tt>forecast mult = current price / SMA20</tt>
 * </p>
 *
 * <p>SMA20 has the same dimension as current price (coins)
 * so the mult is dimensionless.</p>
 *
 * <p>If mult &gt; 1, expect the price to go up ; if mult &lt; 1,
 * expect it to go down.</p>
 *
 * <p>Note: it is a very bad forecast, it will probably be right on
 * the direction only half the time. It has one very good advantage though:
 * the more people follow it, the more stable is the exchange (because
 * the forecast is just a regression to the mean).</p>
 */
public class SharePriceSimpleForecast {

    private final double mult;

    @JsonCreator
    public SharePriceSimpleForecast(double mult) {
        this.mult = mult;
    }

    @JsonValue
    public double getMult() {
        return mult;
    }
}
