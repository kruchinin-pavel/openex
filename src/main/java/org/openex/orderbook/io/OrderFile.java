package org.openex.orderbook.io;

import org.openex.orderbook.Order;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class OrderFile {
    public static Supplier<Stream<Order>> read(String fileName) {
        return read(fileName, new AtomicLong());
    }

    private static Supplier<Stream<Order>> read(String fileName, AtomicLong time) {
        return () -> {
            try {
                return Files.lines(Paths.get(fileName))
                        .map(v -> Parser.get().parseOrder(v, time::incrementAndGet));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static void write(String fileName, Iterable<Order> orders) {
        Iterator<Order> iterator = orders.iterator();
        try {
            Files.write(Paths.get(fileName), (Iterable<String>) () -> new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public String next() {
                    return Encoder.instance.serializeOrder(iterator.next());

                }
            });
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
}
