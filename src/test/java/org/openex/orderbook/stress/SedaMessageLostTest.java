package org.openex.orderbook.stress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openex.orderbook.BookEvent;
import org.openex.orderbook.OrderBook;
import org.openex.orderbook.Trade;
import org.openex.orderbook.seda.OrderBookContainee;
import org.openex.seda.AbstractSedaFactory;
import org.openex.seda.services.Container;
import org.openex.seda.services.MessageEnveloper;
import org.openex.seda.services.MessageSequencer;
import org.openex.seda.services.PlainMemorySedaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
@Ignore
public class SedaMessageLostTest {
    private static final Logger logger = LoggerFactory.getLogger(SedaMessageLostTest.class);
    private static final int REDUNDANCY_COUNT = 3;
    private static final int MAKE_SNAPSHOT_EVERY_MESSAGE_NO = 10;
    private static final AtomicInteger testCount = new AtomicInteger();
    private final List<Trade> sampleTrades = new ArrayList<>();
    private final OrderBook sampleBook = OrderBook.treeMap();
    private final List<Trade> resultTrades = new ArrayList<>();
    private final List<Container<BookEvent, OrderBook>> redundandContainers = new LinkedList<>();
    private MessageEnveloper<BookEvent> orderInput;
    private MessageSequencer<BookEvent> tradesSummarizer;
    private MessageLooser<BookEvent> looser;
    private AbstractSedaFactory<BookEvent, OrderBook> factory;

    public SedaMessageLostTest(Supplier<AbstractSedaFactory<BookEvent, OrderBook>> factory) {
        this.factory = factory.get();
    }

    @Parameterized.Parameters
    public static List<Object[]> params() {
        return Arrays.asList(
                new Object[]{(Supplier<AbstractSedaFactory<BookEvent, OrderBook>>) PlainMemorySedaFactory::create},
                new Object[]{OrderBookContainee.factory("build/SedaMessageLostTest" + testCount.incrementAndGet())}
        );
    }

    @Before
    public void prepare() {
        sampleBook.onTrade(sampleTrades::add);
        orderInput = factory.getEnveloper();
        tradesSummarizer = factory.sequencer();
        for (int i = 0; i < REDUNDANCY_COUNT; i++) {
            Container<BookEvent, OrderBook> ordBook = factory.container("" + i, OrderBookContainee::new);
            ordBook.makeSnapshotEveryMessageNo(MAKE_SNAPSHOT_EVERY_MESSAGE_NO);

            looser = new MessageLooser<>();
            factory.connect(orderInput, looser);
            factory.connect(looser, ordBook);
            factory.connect(ordBook, tradesSummarizer);
            ordBook.start();

            redundandContainers.add(ordBook);
        }
        factory.connect(tradesSummarizer, () -> msg -> msg.forEach(v -> {
            resultTrades.add((Trade) v);
        }));
    }

    @Test
    public void testMessageLostAtNumber() {
        looser.looseMessageSeq = 10;
        RandomOrders.limit(1_000).forEach(order -> {
            logger.info("Order: {}", order);
            sampleBook.accept(order);
            orderInput.send(order);
            validate();
        });
        validate();
    }

    @Test
    public void testMessageLostRandomly() {
        AtomicInteger totalTradesCount = new AtomicInteger();
        looser.messageLostProbability = 0.01;
        RandomOrders.limit(1_000).forEach(order -> {
            logger.info("Order: {}", order);
            sampleBook.accept(order);
            orderInput.send(order);
            totalTradesCount.addAndGet(validate());
        });
        validate();
        Assert.assertTrue(totalTradesCount.get() > 0);
    }

    private int validate() {
        int tradesOccured = sampleTrades.size();
        for (Container<BookEvent, OrderBook> container : redundandContainers) {
            if (!container.isConsistent(tradesSummarizer.getLastSeq())) continue;
            OrderBookContainee ordBook = container.getContainee();
            Assert.assertEquals("Comparing " + ordBook.id, sampleBook, ordBook.book);
        }
        Assert.assertEquals(sampleTrades, resultTrades);
        sampleTrades.clear();
        resultTrades.clear();
        return tradesOccured;
    }

}
