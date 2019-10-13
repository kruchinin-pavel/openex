package org.openex.orderbook.stress;

import org.openex.orderbook.BuySell;
import org.openex.orderbook.Order;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RandomOrders {

    private static final AtomicLong counter = new AtomicLong();

    public static void endless(Function<Order, Boolean> consumer) {
        Iterator<Order> iterator = endless().iterator();
        while (true) {
            if (!consumer.apply(iterator.next())) return;
        }
    }

    private static Iterable<Order> endless() {
        return limit(-1);
    }

    public static Stream<Order> limitStream(long orderNumber) {
        return StreamSupport.stream(limit(orderNumber).spliterator(), false);
    }

    public static Iterable<Order> limit(long orderNumber) {
        final AtomicLong time = new AtomicLong();
        return () -> new Iterator<Order>() {
            @Override
            public boolean hasNext() {
                return orderNumber == -1 || counter.get() < orderNumber;
            }

            @Override
            public Order next() {
                return new Order("" + counter.incrementAndGet(),
                        ThreadLocalRandom.current().nextBoolean() ? BuySell.buy : BuySell.sell,
                        ThreadLocalRandom.current().nextLong(100, 200),
                        ThreadLocalRandom.current().nextLong(1000, 2000),
                        time.incrementAndGet());
            }
        };
    }
}
