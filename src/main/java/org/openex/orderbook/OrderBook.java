package org.openex.orderbook;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Thread unsafe class.
 * Orders are mutable objects.
 */
public class OrderBook implements Consumer<Order>, Cloneable {
    private final AbstractOrderBookSide[] bookBySide = new AbstractOrderBookSide[2];

    private OrderBook(OrderBook book) {
        bookBySide[BuySell.buy.index] = book.bookBySide[BuySell.buy.index].clone();
        bookBySide[BuySell.sell.index] = book.bookBySide[BuySell.sell.index].clone();
    }

    public OrderBook(Function<BuySell, AbstractOrderBookSide> bookSideFactory) {
        bookBySide[BuySell.buy.index] = bookSideFactory.apply(BuySell.buy);
        bookBySide[BuySell.sell.index] = bookSideFactory.apply(BuySell.sell);
    }

    public static OrderBook treeMap() {
        return new OrderBook(bs -> new OrderBookSideMultiMap(bs.comparator));
    }

    public OrderBook onTrade(Consumer<Trade> tradeConsumer) {
        Arrays.stream(bookBySide).forEach(v -> v.onTrades(tradeConsumer));
        return this;
    }

    OrderBook setTime(Supplier<Long> timeSupplier) {
        Arrays.stream(bookBySide).forEach(v -> v.setTime(timeSupplier));
        return this;
    }

    public void accept(final Order _newOrder) {
        Order newOrder = _newOrder.clone();
        bookBySide[newOrder.side.opposite().index].match(newOrder);
        if (newOrder.getRemain() > 0) {
            bookBySide[newOrder.side.index].addOrder(newOrder);
        }
    }

    void print(Consumer<String> writer) {
        Iterator<Order> buyOrdersIterator = bookBySide[BuySell.buy.index].getRestOrders().iterator();
        Iterator<Order> sellOrdersIterator = bookBySide[BuySell.sell.index].getRestOrders().iterator();
        StringBuffer buffer = new StringBuffer();
        while (buyOrdersIterator.hasNext() || sellOrdersIterator.hasNext()) {
            buffer.setLength(0);
            printNextOrder(buyOrdersIterator, buffer);
            buffer.append("\t|");
            printNextOrder(sellOrdersIterator, buffer);
            writer.accept(buffer.toString());
        }
    }

    private void printNextOrder(Iterator<Order> orderIterator, StringBuffer buffer) {
        DecimalFormat sizeDf = (DecimalFormat) NumberFormat.getNumberInstance(Locale.ENGLISH);
        DecimalFormat priceDf = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
        if (orderIterator.hasNext()) {
            Order order = orderIterator.next();
            buffer.append("\t");
            if (order.side == BuySell.sell) {
                buffer.append(priceDf.format(order.price)).append("\t")
                        .append(sizeDf.format(order.getRemain()));

            } else {
                buffer.append(sizeDf.format(order.getRemain())).append("\t")
                        .append(priceDf.format(order.price));
            }
        } else {
            buffer.append("\t\t");
        }
    }

    public String printString() {
        StringBuffer buf = new StringBuffer();
        print(v -> buf.append(v).append("\n"));
        return buf.toString();
    }

    @Override
    public String toString() {
        return "OrderBook{\n" + printString() + "\n}";
    }

    public void clear() {
        Arrays.stream(bookBySide).forEach(AbstractOrderBookSide::clear);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderBook orderBook = (OrderBook) o;
        return Arrays.equals(bookBySide, orderBook.bookBySide);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bookBySide);
    }

    @Override
    public OrderBook clone() {
        return new OrderBook(this);
    }

    public Collection<Order> getRestOrders() {
        List<Order> restBuy = bookBySide[BuySell.buy.index].getRestOrders();
        List<Order> restSell = bookBySide[BuySell.sell.index].getRestOrders();
        List<Order> orders = new ArrayList<>(restBuy.size() + restSell.size());
        orders.addAll(restBuy);
        orders.addAll(restSell);
        return orders;
    }
}

