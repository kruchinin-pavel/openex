package org.openex.orderbook.io;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.openex.orderbook.Order;
import org.openex.orderbook.OrderBook;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

public class ChronicleEncoder {
    public void encode(OrderBook book, BytesOut out) {
        Collection<Order> orders = book.getRestOrders();
        out.writeInt(orders.size());
        orders.forEach(Encoder.instance::serializeOrder);
    }

    public OrderBook decode(BytesIn in, Supplier<Long> time) {
        final OrderBook book = OrderBook.treeMap();
        for (int i = 0, cnt = in.readInt(); i < cnt; i++) {
            book.accept(Parser.instance.parseOrder(Objects.requireNonNull(in.readUtf8()), time));
        }
        return book;
    }

}
