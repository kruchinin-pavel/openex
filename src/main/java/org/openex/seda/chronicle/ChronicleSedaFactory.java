package org.openex.seda.chronicle;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.jetbrains.annotations.NotNull;
import org.openex.seda.AbstractSedaFactory;
import org.openex.seda.message.Envelope;
import org.openex.seda.message.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @param <E> event type
 * @param <S> snapshot type
 */
public class ChronicleSedaFactory<E, S> extends AbstractSedaFactory<E, S> implements AutoCloseable {
    private final Logger logger = LoggerFactory.getLogger(ChronicleSedaFactory.class);
    private final String eventPath;
    private final String snapshotPath;
    private final Function<BytesIn, S> snapshotReader;
    private final Function<BytesIn, E> eventReader;
    private final BiConsumer<S, BytesOut> snapshotWriter;
    private final ChronicleEnvelopeOutputEndpoint<E> eventOut;
    private final ChronicleEnvelopeInputEndpoint<E> eventSrc;
    private final ChronicleInputEndpoint<Snapshot<S>> snapshotSrc;
    private final ChronicleOutputEndpoint<Snapshot<S>> snapshotOut;

    private ChronicleSedaFactory(String eventPath, String snapshotPath,
                                 BiConsumer<E, BytesOut> eventWriter,
                                 Function<BytesIn, E> eventReader,
                                 BiConsumer<S, BytesOut> snapshotWriter,
                                 Function<BytesIn, S> snapshotReader) {
        this.eventPath = eventPath;
        this.snapshotPath = snapshotPath;
        this.snapshotWriter = snapshotWriter;
        this.snapshotReader = snapshotReader;
        this.eventReader = eventReader;
        eventOut = new ChronicleEnvelopeOutputEndpoint<>(this.eventPath, eventWriter);
        eventSrc = new ChronicleEnvelopeInputEndpoint<>(this.eventPath, eventReader);
        if (snapshotPath == null) {
            snapshotOut = null;
            snapshotSrc = null;
        } else {
            snapshotSrc = new ChronicleInputEndpoint<>(this.snapshotPath,
                    v -> {
                        long sequence = v.readLong();
                        S payload = this.snapshotReader.apply(v);
                        return new Snapshot<>(sequence, payload);
                    });

            snapshotOut = new ChronicleOutputEndpoint<>(snapshotPath,
                    (s, v) -> {
                        v.writeLong(s.seq);
                        snapshotWriter.accept(s.payload, v);
                    });
        }
        connect(getEnveloper().get(), getOutput());
    }

    public static void clearPath(String pathStr) {
        if (Files.isDirectory(Paths.get(pathStr))) {
            try {
                Files.walkFileTree(Paths.get(pathStr), new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (!file.toFile().delete()) throw new RuntimeException("Unable to delete: " + file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static <E, S> ChronicleSedaFactory<E, S> create(String eventPath, String snapshotPath,
                                                           BiConsumer<E, BytesOut> eventWriter,
                                                           Function<BytesIn, E> eventReader,
                                                           BiConsumer<S, BytesOut> snapshotWriter,
                                                           Function<BytesIn, S> snapshotReader) {
        return new ChronicleSedaFactory<>(eventPath, snapshotPath,
                eventWriter, eventReader, snapshotWriter, snapshotReader);
    }

    private ChronicleEnvelopeOutputEndpoint<E> getOutput() {
        return eventOut;
    }

    @Override
    protected ChronicleEnvelopeInputEndpoint<E> getInput() {
        return eventSrc;
    }

    @NotNull
    @Override
    protected Supplier<Snapshot<S>> getSnapshotSource() {
        return () -> {
            snapshotSrc.reset();
            while (snapshotSrc.poll() != null) ;
            return snapshotSrc.getLastEvent();
        };
    }

    @NotNull
    @Override
    protected Function<Long, Iterable<Envelope<E>>> getMessageReplayer() {
        return seq -> (Iterable<Envelope<E>>) () -> new Iterator<Envelope<E>>() {
            ChronicleEnvelopeInputEndpoint<E> src =
                    new ChronicleEnvelopeInputEndpoint<>(eventPath, eventReader);

            Envelope<E> lastVal = null;

            @Override
            public boolean hasNext() {
                if (lastVal == null) lastVal = src.poll();
                return lastVal != null;
            }

            @Override
            public Envelope<E> next() {
                hasNext();
                Envelope<E> lastVal = this.lastVal;
                this.lastVal = null;
                return lastVal;
            }
        };
    }

    @NotNull
    @Override
    protected Consumer<Snapshot<S>> getSnapshotStorage() {
        return snapshotOut::send;
    }

    @Override
    public void close() {
        snapshotSrc.close();
        eventSrc.close();
    }

    @Override
    public ExecutorService pollInBackground() {
        final ExecutorService service = Executors.newSingleThreadExecutor();
        service.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                if (getInput().poll() == null) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    logger.info("Polling: got message");
                }
            }
        });
        return service;
    }
}
