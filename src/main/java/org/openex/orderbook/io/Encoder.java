package org.openex.orderbook.io;

import org.openex.orderbook.BookEvent;
import org.openex.orderbook.Order;
import org.openex.orderbook.Trade;


public class Encoder {
    public static final Encoder instance = new Encoder();

    public String encodeEvent(BookEvent event) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(event.getClass().getSimpleName()).append(",");
        if (event instanceof Trade) {
            buffer.append(serializeTrade((Trade) event));
        } else if (event instanceof Order) {
            buffer.append(serializeOrder((Order) event));
        } else throw new IllegalArgumentException("Unknown type: " + event);
        return buffer.toString();
    }

    private String serializeTrade(Trade trade) {
        return trade.aggressiveOrdId + "," +
                trade.restOrdId + "," +
                trade.qty + "," +
                trade.prc + ",";
    }

    public String serializeOrder(Order order) {
        final StringBuilder buffer = new StringBuilder();
        buffer.setLength(0);
        buffer.append(order.orderId);
        buffer.append(",").append(order.side.strRepresentation);
        buffer.append(",").append(order.price);
        buffer.append(",").append(order.getVolume());
        if (order.visibleVolume > 0) buffer.append(",").append(order.visibleVolume);
        return buffer.toString();
    }
}
