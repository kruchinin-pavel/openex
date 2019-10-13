package org.openex.orderbook;

import org.junit.Test;
import org.openex.orderbook.io.Parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ParserTest {

    @Test
    public void testCase1() throws IOException {
        List<Order> orders = Files.readAllLines(Paths.get("src/test/resources/test1.txt"))
                .stream()
                .map(v -> Parser.get().parseOrder(v, () -> 1L))
                .collect(Collectors.toList());

        assertEquals(6, orders.size());
        assertEquals(500, orders.get(2).getVolume());
        assertEquals(BuySell.sell, orders.get(3).side);
        assertEquals(99, orders.get(4).price);
        assertEquals("10004", orders.get(5).orderId);

    }

    @Test
    public void testCase3() throws IOException {
        List<Order> orders = Files.readAllLines(Paths.get("src/test/resources/test3.txt"))
                .stream()
                .map(v -> Parser.get().parseOrder(v, () -> 1L))
                .collect(Collectors.toList());
        assertEquals(10000, orders.get(5).visibleVolume);

    }

}