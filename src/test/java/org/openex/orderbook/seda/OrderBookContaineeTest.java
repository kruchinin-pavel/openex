package org.openex.orderbook.seda;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openex.orderbook.BookEvent;
import org.openex.orderbook.OrderBook;
import org.openex.orderbook.Trade;
import org.openex.orderbook.stress.RandomOrders;
import org.openex.seda.AbstractSedaFactory;
import org.openex.seda.services.Container;
import org.openex.seda.services.MessageEnveloper;
import org.openex.seda.services.MessageSequencer;
import org.openex.seda.services.PlainMemorySedaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class OrderBookContaineeTest {
    private static final Logger logger = LoggerFactory.getLogger(OrderBookContaineeTest.class);
    private final AbstractSedaFactory<BookEvent, OrderBook> factory;

    public OrderBookContaineeTest(AbstractSedaFactory<BookEvent, OrderBook> factory) {
        this.factory = factory;
    }

    @Parameterized.Parameters
    public static List<Object[]> params() {
        return Arrays.asList(
                new Object[]{PlainMemorySedaFactory.create()},
                new Object[]{OrderBookContainee.factory("build/OrderBookContaineeTest")}
        );
    }

    @Test
    public void test() throws InterruptedException {
        List<Trade> sampleTrades = new ArrayList<>();
        OrderBook sampleBook = OrderBook.treeMap();
        sampleBook.onTrade(sampleTrades::add);

        final MessageEnveloper<BookEvent> enveloper = factory.getEnveloper();
        final Container<BookEvent, OrderBook> book1 = factory.container("1", OrderBookContainee::new);
        final Container<BookEvent, OrderBook> book2 = book1.copy("2");
        final Container<BookEvent, OrderBook> book3 = book1.copy("3");
        final MessageSequencer<BookEvent> tradesSummarizer = factory.sequencer();

        factory.connect(factory.input(), book1);
        factory.connect(factory.input(), book2);
        factory.connect(factory.input(), book3);
        factory.connect(book1, tradesSummarizer);
        factory.connect(book2, tradesSummarizer);
        factory.connect(book3, tradesSummarizer);

        book1.start();
        book2.start();

        AtomicLong tradeCounter = new AtomicLong();
        factory.connect(tradesSummarizer, () -> msg -> msg.forEach(p -> {
            if (p instanceof Trade) {
                logger.info("Trade: {}", msg);
                tradeCounter.incrementAndGet();
            }
        }));

        RandomOrders.endless(order -> {
            sampleBook.accept(order);
            enveloper.send(order);
            return sampleTrades.size() == 0;
        });

        factory.pollInBackground();
        Thread.sleep(1_000);
        assertTrue(tradeCounter.get() > 0);

        book1.backupSnapshot();
        RandomOrders.limit(2).forEach(order -> {
            enveloper.send(order);
            sampleBook.accept(order);
        });

        book3.start("1");
        OrderBook ordBook1 = ((OrderBookContainee) book1.getContainee()).book;
        OrderBook ordBook2 = ((OrderBookContainee) book2.getContainee()).book;
        logger.info("sampleBook: {}", sampleBook);
        logger.info("Book1: {}", ordBook1);
        logger.info("Book2: {}", ordBook2);
        assertEquals(sampleBook, ordBook1);
        assertEquals(sampleBook, ordBook2);
    }
}