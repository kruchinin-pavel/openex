package org.openex.orderbook.io;

import org.openex.orderbook.BookEvent;
import org.openex.orderbook.BuySell;
import org.openex.orderbook.Order;
import org.openex.orderbook.Trade;

import java.util.function.Supplier;

public class Parser {

    public static final Parser instance = new Parser();

    public static Parser get() {
        return instance;
    }

    public BookEvent parseEvent(String s, Supplier<Long> time) {
        String[] tokens = s.split(",");
        if (tokens[0].equals(Trade.class.getSimpleName())) {
            return parseTrade(s.substring(s.indexOf(",") + 1));
        } else if (tokens[0].equals(Order.class.getSimpleName())) {
            return parseOrder(s.substring(s.indexOf(",") + 1), time);
        }
        throw new IllegalArgumentException("Unknown string to parse: " + s);
    }

    private Trade parseTrade(String s) {
        String[] tokens = s.split(",");
        return new Trade(tokens[0], tokens[1], Long.parseLong(tokens[2]), Long.parseLong(tokens[3]));
    }

    public Order parseOrder(String s, Supplier<Long> time) {
        String[] tokens = s.split(",");
        long volume = Long.parseLong(tokens[3].trim());
        return new Order(tokens[0],
                BuySell.by(tokens[1]),
                Long.parseLong(tokens[2].trim()),
                volume,
                time.get(),
                tokens.length < 5 ? volume : Long.parseLong(tokens[4]));
    }
}
