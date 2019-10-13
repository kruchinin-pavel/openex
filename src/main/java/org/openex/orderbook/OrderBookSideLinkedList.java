package org.openex.orderbook;

import java.util.*;

public class OrderBookSideLinkedList extends AbstractOrderBookSide {
    private final LinkedList<Order> restOrders = new LinkedList<>();

    @Override
    public AbstractOrderBookSide clone() {
        OrderBookSideLinkedList ret = new OrderBookSideLinkedList();
        restOrders.forEach(o -> ret.restOrders.add(o.clone()));
        return ret;
    }

    public void addOrder(Order order) {
        for (ListIterator<Order> iter = restOrders.listIterator(); iter.hasNext(); ) {
            Order restOrder = iter.next();
            if (restOrder.side.sideOrder(restOrder.price, order.price) > 0) {
                iter.previous();
                iter.add(order);
                return;
            }
        }
        restOrders.addLast(order);
    }

    public void match(Order takeOrder) {
        List<Trade> rawTrades = new ArrayList<>();
        long taken;
        do {
            taken = 0;
            for (Iterator<Order> iter = restOrders.iterator(); iter.hasNext() && takeOrder.getRemain() > 0; ) {
                Order restOrder = iter.next();
                if ((taken = restOrder.tryMatch(takeOrder)) == 0) break;
                rawTrades.add(new Trade(takeOrder.orderId, restOrder.orderId, restOrder.price, taken));
                if (restOrder.getRemain() == 0) {
                    iter.remove();
                    Order restIceberg;
                    if ((restIceberg = restOrder.getNewPeak(getTime())) != null) {
                        addOrder(restIceberg);
                        break;
                    }
                }
            }
        } while (taken > 0);
        rawTrades.stream().collect(Trade.groupBySellerBuyer).forEach((counterparts, trades) ->
                trades.stream().reduce(Trade::add).ifPresent(getTradeConsumer()));
    }


    public List<Order> getRestOrders() {
        return Collections.unmodifiableList(restOrders);
    }

    public void clear() {
        restOrders.clear();
    }
}
