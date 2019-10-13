package org.openex.seda.chronicle;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public class ChronicleInputEndpoint<T> implements AutoCloseable {
    private final String path;
    private final Function<BytesIn, T> reader;
    private final AtomicBoolean init = new AtomicBoolean();
    private final AtomicReference<T> lastEvent = new AtomicReference<>();
    private final Collection<Consumer<T>> consumers = new LinkedHashSet<>();
    private ChronicleQueue queue;
    private ExcerptTailer tailer;

    public ChronicleInputEndpoint(String path, Function<BytesIn, T> reader) {
        this.path = path;
        this.reader = reader;
    }

    public T getLastEvent() {
        return lastEvent.get();
    }

    void reset() {
        init.set(false);
    }

    public T poll() {
        synchronized (this) {
            if (init.compareAndSet(false, true)) {
                queue = ChronicleQueue.singleBuilder(path).build();
                tailer = queue.createTailer();
            }
        }
        if (!tailer.peekDocument()) return null;
        try (DocumentContext dc = tailer.readingDocument()) {
            Wire wire = dc.wire();
            if (wire != null) {
                wire.readBytes(bytes -> {
                    T v = reader.apply(bytes);
                    consumers.forEach(a -> a.accept(v));
                    lastEvent.set(v);
                });
            }
        }
        return lastEvent.get();
    }

    public boolean subscribe(Consumer<T> consumer) {
        return consumers.add(consumer);
    }

    @Override
    public void close() {
        queue.close();
    }

}
