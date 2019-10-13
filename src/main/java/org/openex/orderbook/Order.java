package org.openex.orderbook;

import java.util.Objects;
import java.util.function.Supplier;

public class Order implements Comparable<Order>, Cloneable, BookEvent {
    public final long price;
    public final BuySell side;
    public final String orderId;
    public final long visibleVolume;
    private final long time;
    private final long volume;
    private long remain;
    private long deepRemain;

    public Order(String orderId, BuySell side, long price, long volume, long time) {
        this(orderId, side, price, volume, time, volume);
    }

    public Order(String orderId, BuySell side, long price, long volume, long time, long visibleVolume) {
        this(orderId, side, price, volume, volume, time, visibleVolume);
    }

    private Order(String orderId, BuySell side, long price, long volume, long remain, long time, long visibleVolume) {
        this.orderId = orderId;
        this.side = side;
        this.price = price;
        this.time = time;
        this.volume = volume;
        this.remain = remain;
        this.visibleVolume = visibleVolume;
        this.deepRemain = Math.max(remain - visibleVolume, 0);
        this.remain -= deepRemain;
    }

    @Override
    public int compareTo(Order o) {
        if (o.side != side) {
            throw new IllegalArgumentException("Can't compare orders with different sides");
        }
        int result = o.side.sideOrder(price, o.price);
        return result == 0 ? Long.compare(time, o.time) : result;
    }

    long tryMatch(Order takeOrder) {
        if (side == takeOrder.side) {
            return 0;
        }
        long matchedSize = 0;
        if (side == BuySell.buy && price >= takeOrder.price ||
                side == BuySell.sell && price <= takeOrder.price) {
            matchedSize = Math.min(getRemain(), takeOrder.remain + takeOrder.deepRemain);
        }
        if (matchedSize > 0) {
            takeOrder.deepRemain -= matchedSize;
            if (takeOrder.deepRemain < 0) {
                takeOrder.remain += takeOrder.deepRemain;
                takeOrder.deepRemain = 0;
            }
            remain -= matchedSize;
        }
        return matchedSize;
    }

    Order getNewPeak(Supplier<Long> time) {
        return deepRemain == 0 || remain > 0 ? null :
                new Order(orderId, side, price, volume, deepRemain, time.get(), visibleVolume);
    }

    public long getVolume() {
        return volume;
    }

    long getRemain() {
        return Math.min(remain, visibleVolume);
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + orderId +
                ", side=" + side +
                ", prc=" + price +
                ", volume=" + volume +
                ", remain=" + remain +
                ", deepRemain=" + deepRemain +
                ", visibleRemain=" + getRemain() +
                '}';
    }

    @Override
    public Order clone() {
        try {
            return (Order) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return remain == order.remain &&
                deepRemain == order.deepRemain &&
                price == order.price &&
                time == order.time &&
                volume == order.volume &&
                visibleVolume == order.visibleVolume &&
                side == order.side &&
                Objects.equals(orderId, order.orderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remain, deepRemain, price, time, side, volume, orderId, visibleVolume);
    }
}
