package org.openex.orderbook;

import java.util.Comparator;

public enum BuySell {
    buy(0, null, "B", (o1, o2) -> Long.compare(o2, o1)),
    sell(1, buy, "S", Long::compare);

    static {
        buy.opposite = sell;
    }

    public final int index;
    public final String strRepresentation;
    public final Comparator<Long> comparator;
    private BuySell opposite;

    BuySell(int index, BuySell opposite, String strRepresentation, Comparator<Long> priceComparator) {
        this.index = index;
        this.opposite = opposite;
        this.strRepresentation = strRepresentation;
        this.comparator = priceComparator;
    }

    public static BuySell by(String str) {
        for (BuySell value : values()) {
            if (value.strRepresentation.equals(str)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Not found value for [" + str + "]");
    }

    public BuySell opposite() {
        return opposite;
    }

    public int sideOrder(long price1, long price2) {
        return comparator.compare(price1, price2);
    }
}
