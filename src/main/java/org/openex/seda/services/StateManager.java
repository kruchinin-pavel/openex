package org.openex.seda.services;

import org.openex.seda.interfaces.ContainerState;
import org.openex.seda.message.Envelope;
import org.openex.seda.message.Snapshot;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class StateManager<E, S> {
    private final Container<E, S> container;
    private final AtomicReference<ContainerState> state = new AtomicReference<>(ContainerState.NotRunning);
    private Consumer<Snapshot<S>> snapshotConsumer;
    private Supplier<Snapshot<S>> snapshotProducer;
    private Function<Long, Iterable<Envelope<E>>> messageReplay;

    StateManager(Container<E, S> container, StateManager<E, S> copyFrom) {
        this.container = container;
        if (copyFrom != null) {
            snapshotConsumer = copyFrom.snapshotConsumer;
            snapshotProducer = copyFrom.snapshotProducer;
            messageReplay = copyFrom.messageReplay;
        }
    }

    void restart(String fromSnapshotId) {
        implRestart(fromSnapshotId, ContainerState.Running);
    }

    void start(String fromSnapshotId) {
        implRestart(fromSnapshotId, ContainerState.NotRunning);
    }

    private void implRestart(String fromSnapshotId, ContainerState baseState) {
        if (state.compareAndSet(baseState, ContainerState.Initializing)) {
            try {
                final Snapshot snapshotEnvelope = snapshotProducer.get();
                long startSeq = 0;
                if (snapshotEnvelope == null) container.setSnapshot(null);
                else {
                    container.setSnapshot(snapshotEnvelope);
                    startSeq = snapshotEnvelope.seq;
                }

                if (messageReplay != null) {
                    container.replay(messageReplay.apply(startSeq).iterator());
                }
                state.set(ContainerState.Running);
            } catch (Exception e) {
                state.set(ContainerState.NotRunning);
                throw new RuntimeException(e);
            }
        }
    }

    void setSnapshotConsumer(Consumer<Snapshot<S>> snapshotConsumer) {
        this.snapshotConsumer = snapshotConsumer;
    }

    void setSnapshotProducer(Supplier<Snapshot<S>> snapshotProducer) {
        this.snapshotProducer = snapshotProducer;
    }

    void setMessageReplay(Function<Long, Iterable<Envelope<E>>> messageReplay) {
        this.messageReplay = messageReplay;
    }

    void set(Snapshot<S> snapshot) {
        snapshotConsumer.accept(snapshot);
    }

    void stop() {
        state.set(ContainerState.NotRunning);
    }

    ContainerState getState() {
        return state.get();
    }

    @Override
    public String toString() {
        return "StateManager{" +
                "container=" + container +
                ", state=" + state +
                '}';
    }
}
