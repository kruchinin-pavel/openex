package org.openex.orderbook;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Trade implements BookEvent {
    static final Collector<Trade, ?, LinkedHashMap<String, List<Trade>>> groupBySellerBuyer = Collectors.groupingBy(
            trade -> trade.aggressiveOrdId + trade.restOrdId,
            LinkedHashMap::new,
            Collectors.mapping(Function.identity(), Collectors.toList())
    );
    public final String aggressiveOrdId;
    public final String restOrdId;
    public final long prc;
    public final long qty;

    public Trade(String aggressiveOrdId, String restOrdId, long prc, long qty) {
        this.aggressiveOrdId = aggressiveOrdId;
        this.restOrdId = restOrdId;
        this.prc = prc;
        this.qty = qty;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "aggressiveOrdId=" + aggressiveOrdId +
                ", restOrdId=" + restOrdId +
                ", prc=" + prc +
                ", qty=" + qty +
                '}';
    }

    void print(Consumer<String> consumer) {
        consumer.accept(String.format("trade %s,%s,%s,%s", aggressiveOrdId, restOrdId, prc, qty));
    }

    public Trade add(Trade that) {
        if (!aggressiveOrdId.equals(that.aggressiveOrdId) || !restOrdId.equals(that.restOrdId)) {
            throw new IllegalArgumentException(String.format("Not equal trades: this: %s that: %s", this, that));
        }
        return new Trade(aggressiveOrdId, restOrdId, prc, qty + that.qty);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Trade trade = (Trade) o;
        return prc == trade.prc &&
                qty == trade.qty &&
                Objects.equals(aggressiveOrdId, trade.aggressiveOrdId) &&
                Objects.equals(restOrdId, trade.restOrdId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggressiveOrdId, restOrdId, prc, qty);
    }
}
