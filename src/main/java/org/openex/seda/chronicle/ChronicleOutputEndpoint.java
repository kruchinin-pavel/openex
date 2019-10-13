package org.openex.seda.chronicle;

import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;


public class ChronicleOutputEndpoint<T> implements AutoCloseable {
    private final String path;
    private final BiConsumer<T, BytesOut> serializer;
    private final AtomicBoolean inited = new AtomicBoolean();
    private SingleChronicleQueue build;
    private ExcerptAppender appender;

    public ChronicleOutputEndpoint(String path, BiConsumer<T, BytesOut> serializer) {
        this.path = path;
        this.serializer = serializer;
    }

    public void send(T event) {
        synchronized (this) {
            if (inited.compareAndSet(false, true)) {
                build = ChronicleQueue.singleBuilder(path).build();
                appender = build.acquireAppender();
            }
        }
        try (final DocumentContext dc = appender.writingDocument()) {
            Objects.requireNonNull(dc.wire()).writeBytes(out -> writeEvent(event, out));
        }
    }

    private void writeEvent(T event, BytesOut out) {
        serializer.accept(event, out);
    }

    @Override
    public void close() {
        build.close();
    }

}
