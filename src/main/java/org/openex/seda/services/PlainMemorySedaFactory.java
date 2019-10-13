package org.openex.seda.services;

import org.jetbrains.annotations.NotNull;
import org.openex.seda.AbstractSedaFactory;
import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;
import org.openex.seda.message.Snapshot;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class PlainMemorySedaFactory<E, S> extends AbstractSedaFactory<E, S> {
    private final MessageBank<E> messageBank = new MessageBank<>();
    private final SnapshotKeeper snapshotKeeper = new SnapshotKeeper();

    private PlainMemorySedaFactory() {
        connect(getEnveloper().get(), messageBank.getInput());
    }

    public static <E, S> PlainMemorySedaFactory<E, S> create() {
        return new PlainMemorySedaFactory<>();
    }

    @Override
    protected InputEndpoint<E> getInput() {
        return getEnveloper().get();
    }

    @NotNull
    protected Supplier<Snapshot<S>> getSnapshotSource() {
        return snapshotKeeper::getSnapshot;
    }

    @NotNull
    protected Function<Long, Iterable<Envelope<E>>> getMessageReplayer() {
        return messageBank::replay;
    }

    @NotNull
    protected Consumer<Snapshot<S>> getSnapshotStorage() {
        return snapshot -> {
            snapshotKeeper.putSnapshot(snapshot);
            messageBank.truncateBySeq(snapshot.seq);
        };
    }

    public ExecutorService pollInBackground() {
        return null;
    }
}
