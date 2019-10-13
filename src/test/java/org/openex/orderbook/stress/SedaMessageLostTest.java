package org.openex.orderbook.stress;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openex.orderbook.BookEvent;
import org.openex.orderbook.Order;
import org.openex.orderbook.OrderBook;
import org.openex.seda.AbstractSedaFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class SedaMessageLostTest extends SedaMessageLostAbstract {

    public SedaMessageLostTest(Supplier<AbstractSedaFactory<BookEvent, OrderBook>> factory,
                               Supplier<Stream<Order>> orders) {
        super(factory, orders);
    }

    @Test
    public void testMessageLostAtNumber() {
        looser.looseMessageSeq = 10;
        AtomicInteger iteration = new AtomicInteger();
        orders.forEach(order -> {
            logger.info("Order: {}", order);
            sampleBook.accept(order);
            orderInput.send(order);
            validate();
        });
        validate();
    }


}
