package org.openex.orderbook;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class AbstractOrderBookSide implements Cloneable {
    private Consumer<Trade> tradeConsumer = trade -> {
    };
    private Supplier<Long> time;

    void onTrades(Consumer<Trade> tradeConsumer) {
        this.tradeConsumer = tradeConsumer;
    }

    abstract void addOrder(Order order);

    abstract void match(Order takeOrder);

    abstract List<Order> getRestOrders();

    abstract void clear();

    Consumer<Trade> getTradeConsumer() {
        return tradeConsumer;
    }

    Supplier<Long> getTime() {
        return time;
    }

    <T extends AbstractOrderBookSide> T setTime(Supplier<Long> time) {
        this.time = time;
        return (T) this;
    }

    @Override
    public abstract AbstractOrderBookSide clone();
}
