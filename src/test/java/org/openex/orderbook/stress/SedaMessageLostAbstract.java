package org.openex.orderbook.stress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.runners.Parameterized;
import org.openex.orderbook.BookEvent;
import org.openex.orderbook.Order;
import org.openex.orderbook.OrderBook;
import org.openex.orderbook.Trade;
import org.openex.orderbook.io.OrderFile;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SedaMessageLostAbstract {
    private static final int REDUNDANCY_COUNT = 3;
    private static final int MAKE_SNAPSHOT_EVERY_MESSAGE_NO = 10;
    private static final AtomicInteger testCount = new AtomicInteger();
    private static final AtomicInteger constCount = new AtomicInteger();
    protected final Logger logger;
    protected final OrderBook sampleBook = OrderBook.treeMap();
    protected final Stream<Order> orders;
    private final long instance;
    private final List<Trade> sampleTrades = new ArrayList<>();
    private final List<Trade> resultTrades = new ArrayList<>();
    private final List<Container<BookEvent, OrderBook>> redundandContainers = new LinkedList<>();
    protected MessageEnveloper<BookEvent> orderInput;
    protected MessageLooser<BookEvent> looser;
    private MessageSequencer<BookEvent> tradesSummarizer;
    private AbstractSedaFactory<BookEvent, OrderBook> factory;

    public SedaMessageLostAbstract(Supplier<AbstractSedaFactory<BookEvent, OrderBook>> factory,
                                   Supplier<Stream<Order>> orders) {
        this.factory = factory.get();
        this.orders = orders.get();
        instance = constCount.incrementAndGet();
        logger = LoggerFactory.getLogger(getClass() + ":" + instance);
        logger.info("Starting test: {}", instance);
    }

    @Parameterized.Parameters
    public static List<Object[]> params() {
        Supplier<Stream<Order>> fixedOrders = OrderFile.read("src/test/external_resources/inputOrders.csv");
        List<Order> randomOrderList = RandomOrders.limitStream(1_000).collect(Collectors.toList());
        Supplier<Stream<Order>> randomOrders = randomOrderList::stream;
        return Arrays.asList(
                new Object[]{(Supplier<AbstractSedaFactory<BookEvent, OrderBook>>) PlainMemorySedaFactory::create,
                        fixedOrders},
                new Object[]{(Supplier<AbstractSedaFactory<BookEvent, OrderBook>>) PlainMemorySedaFactory::create,
                        randomOrders}//,
//                new Object[]{OrderBookContainee.factory("build/SedaMessageLostTest_" + testCount.incrementAndGet()),
//                        fixedOrders},
//                new Object[]{OrderBookContainee.factory("build/SedaMessageLostTest_" + testCount.incrementAndGet()),
//                        randomOrders}
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

    protected int validate() {
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
