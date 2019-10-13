package org.openex.orderbook;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class OrderBookSideArrayList extends AbstractOrderBookSide {
    private final List<Order> restOrders = new ArrayList<>();

    @Override
    public AbstractOrderBookSide clone() {
        OrderBookSideArrayList list = new OrderBookSideArrayList();
        restOrders.forEach(v -> list.restOrders.add(v.clone()));
        return list;
    }

    @Override
    public void addOrder(Order order) {
        restOrders.add(order);
        Collections.sort(restOrders);
    }

    @Override
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


    @Override
    public List<Order> getRestOrders() {
        return Collections.unmodifiableList(restOrders);
    }

    @Override
    public void clear() {
        restOrders.clear();
    }
}
