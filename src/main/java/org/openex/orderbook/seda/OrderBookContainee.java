package org.openex.orderbook.seda;

import org.openex.orderbook.BookEvent;
import org.openex.orderbook.Order;
import org.openex.orderbook.OrderBook;
import org.openex.orderbook.io.Encoder;
import org.openex.orderbook.io.Parser;
import org.openex.seda.chronicle.ChronicleSedaFactory;
import org.openex.seda.message.Envelope;
import org.openex.seda.message.Snapshot;
import org.openex.seda.services.Containee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class OrderBookContainee extends Containee<BookEvent, OrderBook> {
    private final Logger log;
    public OrderBook book = OrderBook.treeMap();
    private Envelope<BookEvent> lastMessage;

    public OrderBookContainee(String id) {
        super(id);
        setSnapshot(null);
        log = LoggerFactory.getLogger(OrderBookContainee.class + ":" + id);
    }

    public static ChronicleSedaFactory<BookEvent, OrderBook> factory(String path) {
        String eventPath = path + "_evt";
        String snapshotPath = path + "_snp";
        ChronicleSedaFactory.clearPath(eventPath);
        ChronicleSedaFactory.clearPath(snapshotPath);
        final AtomicLong time = new AtomicLong();
        return ChronicleSedaFactory.create(
                eventPath,
                snapshotPath,
                (v, out) -> out.writeUtf8(Encoder.instance.encodeEvent(v)),
                in -> Parser.instance.parseEvent(requireNonNull(in.readUtf8()), time::incrementAndGet),
                (v, out) -> {
                    Collection<Order> restOrders = v.getRestOrders();
                    out.writeInt(restOrders.size());
                    restOrders.forEach(ord -> out.writeUtf8(Encoder.instance.serializeOrder(ord)));
                },
                in -> {
                    int count = in.readInt();
                    OrderBook book = OrderBook.treeMap();
                    for (int i = 0; i < count; i++) {
                        book.accept(Parser.instance.parseOrder(requireNonNull(in.readUtf8()), time::incrementAndGet));
                    }
                    return book;
                });
    }

    @Override
    public OrderBook getSnapshot() {
        return book.clone();
    }

    @Override
    public void setSnapshot(Snapshot<OrderBook> snapshot) {
        book = OrderBook.treeMap();
        if (snapshot != null) {
            book = snapshot.payload.clone();
        }
        book.onTrade(event -> {
            log.info("Trade: {}", event);
            send(event);
        });
    }

    @Override
    protected void implProcess(Envelope<BookEvent> envelope) {
        envelope.forEach(msg -> {
            if (msg instanceof Order) {
                book.accept(((Order) msg).clone());
            }
            lastMessage = envelope;
        });
    }

    @Override
    public String toString() {
        return "OrderBookContainee{" +
                "id=" + id +
                ", lastMessage=" + lastMessage +
                ", book=" + book +
                '}';
    }
}
