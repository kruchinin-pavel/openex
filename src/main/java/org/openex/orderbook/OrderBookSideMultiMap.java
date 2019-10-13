package org.openex.orderbook;

import java.util.*;
import java.util.stream.Collectors;

public class OrderBookSideMultiMap extends AbstractOrderBookSide {
    private final TreeMap<Long, LinkedList<Order>> restOrderMap;

    public OrderBookSideMultiMap(Comparator<Long> cmp) {
        restOrderMap = new TreeMap<>(cmp);
    }

    @Override
    public AbstractOrderBookSide clone() {
        OrderBookSideMultiMap ret = new OrderBookSideMultiMap((Comparator<Long>) restOrderMap.comparator());
        restOrderMap.forEach((k, v) -> {
            LinkedList<Order> lst = new LinkedList<>();
            v.forEach(o -> lst.add(o.clone()));
            ret.restOrderMap.put(k, lst);
        });
        return ret;
    }

    public void addOrder(Order order) {
        restOrderMap.computeIfAbsent(order.price, prc -> new LinkedList<>()).addLast(order);
    }

    public void match(Order takeOrder) {
        List<Trade> rawTrades = new ArrayList<>();
        long taken;
        do {
            taken = 0;
            loop:
            for (Iterator<LinkedList<Order>> mapIter = restOrderMap.values().iterator(); mapIter.hasNext() && takeOrder.getRemain() > 0; ) {
                LinkedList<Order> restOrders = mapIter.next();
                for (Iterator<Order> iter = restOrders.iterator(); iter.hasNext() && takeOrder.getRemain() > 0; ) {
                    Order restOrder = iter.next();
                    if ((taken = restOrder.tryMatch(takeOrder)) == 0) break;
                    rawTrades.add(new Trade(takeOrder.orderId, restOrder.orderId, restOrder.price, taken));
                    if (restOrder.getRemain() == 0) {
                        iter.remove();
                        Order restIceberg;
                        if ((restIceberg = restOrder.getNewPeak(getTime())) != null) {
                            addOrder(restIceberg);
                            break loop;
                        }
                    }
                }
            }
        } while (taken > 0);
        rawTrades.stream().collect(Trade.groupBySellerBuyer).forEach((counterparts, trades) ->
                trades.stream().reduce(Trade::add).ifPresent(getTradeConsumer()));
    }


    public List<Order> getRestOrders() {
        return restOrderMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public void clear() {
        restOrderMap.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderBookSideMultiMap that = (OrderBookSideMultiMap) o;
        return Objects.equals(restOrderMap, that.restOrderMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(restOrderMap);
    }
}
