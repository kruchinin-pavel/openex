package org.openex.orderbook.stress;

import org.openex.orderbook.AbstractOrderBookSide;
import org.openex.orderbook.BuySell;
import org.openex.orderbook.Order;
import org.openex.orderbook.OrderBook;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

class StressTestTask {
    private final OrderBook book;
    private final Supplier<Stream<Order>> orders;
    private String className;
    private long ordersPerSec;
    private long tradeCount = 0;
    private String bookStringRepresentation;

    StressTestTask(Function<BuySell, AbstractOrderBookSide> v, Supplier<Stream<Order>> orders) {
        this.orders = orders;
        book = new OrderBook(bS -> {
            AbstractOrderBookSide apply = v.apply(bS);
            className = apply.getClass().getSimpleName();
            return apply;
        }).onTrade(t -> tradeCount++);
    }

    void run() {
        try {
            implRun(true);
            Thread.sleep(1000);
            implRun(false);
            bookStringRepresentation = book.printString();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void implRun(boolean warm) {
        AtomicLong counter = new AtomicLong();
        try {
            book.clear();
            tradeCount = 0;
            long lt = System.nanoTime();
            orders.get().forEach(o -> {
                counter.incrementAndGet();
                book.accept(o);
            });
            long ct = System.nanoTime();
            long runtime = (ct - lt) / 1_000;
            ordersPerSec = (long) (counter.get() * 1e6 / runtime);
            if (!warm) {
                System.out.println(String.format("%s Throughput: total=%s millis, or=%sitems, timing=%s uS/order, " +
                                "speed=%s order/sec, trades=%sitems",
                        className, runtime / 1_000, counter.get(), runtime / counter.get(), ordersPerSec, tradeCount));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error on order count: " + counter.get() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StressTestTask that = (StressTestTask) o;
        return Objects.equals(tradeCount, that.tradeCount) &&
                Objects.equals(bookStringRepresentation, that.bookStringRepresentation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tradeCount, bookStringRepresentation);
    }

    @Override
    public String toString() {
        return "StressTestTask{" +
                "className='" + className + '\'' +
                ", ordersPerSec=" + ordersPerSec +
                ", tradeCount=" + tradeCount +
                '}';
    }
}
