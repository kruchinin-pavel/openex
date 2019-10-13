package org.openex.orderbook.stress;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openex.orderbook.BookEvent;
import org.openex.orderbook.Order;
import org.openex.orderbook.OrderBook;
import org.openex.orderbook.Trade;
import org.openex.orderbook.io.OrderFile;
import org.openex.orderbook.seda.OrderBookContainee;
import org.openex.seda.AbstractSedaFactory;
import org.openex.seda.message.Envelope;
import org.openex.seda.services.Container;
import org.openex.seda.services.MessageEnveloper;
import org.openex.seda.services.MessageSequencer;
import org.openex.seda.services.PlainMemorySedaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SedaMessageLostTest {
    private static final AtomicInteger instCount = new AtomicInteger();
    private final Logger logger;
    private final List<Order> orders;
    private final OrderBook sampleBook = OrderBook.treeMap();
    private final int instance = instCount.getAndIncrement();
    private final List<Trade> sampleTrades = new ArrayList<>();
    private final List<Trade> resultTrades = new ArrayList<>();

    public SedaMessageLostTest(Function<Integer, AbstractSedaFactory<BookEvent, OrderBook>> factorySup,
                               Supplier<Stream<Order>> ordersSup,
                               Function<Envelope<BookEvent>, Boolean> func) throws IOException {

        this.factory = factorySup.apply(instance);
        this.orders = ordersSup.get().collect(Collectors.toList());
        Path path = Paths.get("build/SedaMessageLostTest/inst_" + instance);
        Files.createDirectories(path);
        OrderFile.write("build/SedaMessageLostTest/inst_" + instance + "/orders.csv", orders);
        logger = LoggerFactory.getLogger(getClass() + ":" + instance);
        logger.info("Starting test: {}", instance);
        sampleBook.onTrade(sampleTrades::add);
        orderInput = factory.getEnveloper();
        tradesSummarizer = factory.sequencer();
        for (int i = 0; i < REDUNDANCY_COUNT; i++) {
            Container<BookEvent, OrderBook> ordBook = factory.container("" + i, OrderBookContainee::new);
            ordBook.makeSnapshotEveryMessageNo(MAKE_SNAPSHOT_EVERY_MESSAGE_NO);
            MessageLooser<BookEvent> looser = new MessageLooser<>();
            if (i == REDUNDANCY_COUNT - 1) looser.setLoseFunction(v -> {
                Boolean ret = func.apply(v);
                if (ret) {
                    try {
                        Files.write(Paths.get(path.toString(), "skipped.csv"), Collections.singleton("" + v.seq));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return ret;
            });
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

    private static final int REDUNDANCY_COUNT = 3;
    private static final int MAKE_SNAPSHOT_EVERY_MESSAGE_NO = 10;
    private final List<Container<BookEvent, OrderBook>> redundandContainers = new LinkedList<>();
    private final MessageEnveloper<BookEvent> orderInput;
    private final MessageSequencer<BookEvent> tradesSummarizer;
    private final AbstractSedaFactory<BookEvent, OrderBook> factory;

    @Parameterized.Parameters
    public static List<Object[]> params() {
        List<Function<Integer, AbstractSedaFactory<BookEvent, OrderBook>>> factories = Arrays.asList(
                instance -> PlainMemorySedaFactory.create(),
                instance -> OrderBookContainee.factory("build/SedaMessageLostTest/inst_" + instance + "/chronicle")
        );
        List<Order> randomOrderList = RandomOrders.limitStream(1_000).collect(Collectors.toList());
        List<Supplier<Stream<Order>>> orders = Arrays.asList(
                OrderFile.read("src/test/external_resources/inputOrders.csv"),
                randomOrderList::stream);
        List<Function<Envelope<BookEvent>, Boolean>> lostFuncs = Arrays.asList(
                v -> ThreadLocalRandom.current().nextDouble() < 0.01, v -> v.seq == 113);
        List<Object[]> params = new ArrayList<>();
        factories.forEach(factory -> lostFuncs.forEach(lostFunc -> orders.forEach(ord ->
                params.add(new Object[]{factory, ord, lostFunc}))));
        return params;
    }

    @Test
    public void testMessageLostRandomly() {
        AtomicInteger totalTradesCount = new AtomicInteger();
        orders.forEach(order -> {
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
            assertEquals("Comparing " + ordBook.id, sampleBook, ordBook.book);
        }
        assertEquals(sampleTrades, resultTrades);
        sampleTrades.clear();
        resultTrades.clear();
        return tradesOccured;
    }
}
