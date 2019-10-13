package org.openex.orderbook.stress;

import org.openex.orderbook.*;
import org.openex.orderbook.io.OrderFile;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StressTest {
    private static final int orderNumber = 1_024 * 1_024;
    private static final String fileName = "build/dump.orders";
    private final List<StressTestTask> results;

    @SafeVarargs
    private StressTest(String fileName, Function<BuySell, AbstractOrderBookSide>... sideSuppliers) {
        results = Arrays.stream(sideSuppliers).map(v -> new StressTestTask(v, OrderFile.read(fileName)))
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        System.out.println("Running stress test tasks.");
        OrderFile.write(fileName, RandomOrders.limit(orderNumber));
        new StressTest(fileName,
                bS -> new OrderBookSideArrayList(),
                bs -> new OrderBookSideLinkedList(),
                bs -> new OrderBookSideMultiMap(bs.comparator)
        ).run();
        System.out.println("Stress test tasks completed.");
    }

    private StressTest run() {
        Set<StressTestTask> uniqueSet = new HashSet<>();
        results.forEach(stressTestTask -> {
            stressTestTask.run();
            if (uniqueSet.size() == 0) uniqueSet.add(stressTestTask);
            if (uniqueSet.add(stressTestTask)) {
                throw new IllegalStateException("Found not equivalent results in set: " + uniqueSet);
            }
        });
        return this;
    }

    @Override
    public String toString() {
        return "StressTest{" +
                "results=" + results +
                '}';
    }


}
