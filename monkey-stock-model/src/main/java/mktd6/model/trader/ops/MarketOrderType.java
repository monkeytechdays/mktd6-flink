package mktd6.model.trader.ops;

public enum MarketOrderType {

    BUY(1),
    SELL(-1);

    private final int buySign;

    MarketOrderType(int i) {
        buySign = i;
    }

    public int getShareSign() {
        return buySign;
    }

    public int getCoinSign() {
        return buySign * -1;
    }

}
