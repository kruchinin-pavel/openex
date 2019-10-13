package org.openex.orderbook;

import org.openex.orderbook.io.Parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class ConsoleProcessor {
    private final List<String> tradesStr = new ArrayList<>();
    private final AtomicLong time = new AtomicLong();
    private final OrderBook book;

    private ConsoleProcessor(Function<BuySell, AbstractOrderBookSide> bookSideFactory) {
        book = new OrderBook(bookSideFactory)
                .onTrade(trade -> trade.print(v -> tradesStr.add(v.trim())))
                .setTime(time::get);
    }

    public static void main(String[] args) throws IOException {
        ConsoleProcessor consoleProcessor = ConsoleProcessor.arrayListConsole();
        String inpStr;
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while ((inpStr = reader.readLine()) != null) {
            consoleProcessor.consume(inpStr);
        }
        consoleProcessor.print(System.out::println);
    }

    public static ConsoleProcessor arrayListConsole() {
        return new ConsoleProcessor(bS -> new OrderBookSideArrayList());
    }

    public static ConsoleProcessor linkedListConsole() {
        return new ConsoleProcessor(bS -> new OrderBookSideLinkedList());
    }

    public static ConsoleProcessor multiMapConsole() {
        return new ConsoleProcessor(bS -> new OrderBookSideMultiMap(bS.comparator));
    }

    public void consume(String str) {
        book.accept(Parser.get().parseOrder(str, time::incrementAndGet));
    }

    public void print(Consumer<String> writer) {
        tradesStr.forEach(writer);
        book.print(writer);
    }

}
