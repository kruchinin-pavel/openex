package org.openex.seda.services;

import org.openex.seda.interfaces.ContainerState;
import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;
import org.openex.seda.message.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Supports full cycle of messaging and state processing for containee object
 */
public class Container<E, S> implements InputGetter<E>, OutputGetter<E> {
    private static final Logger log = LoggerFactory.getLogger(Container.class);
    private final String id;
    private final Function<String, Containee<E, S>> factory;
    private final StateManager<E, S> stateManager;
    private final MessageSequencer<E> sequencer = new MessageSequencer<>();
    private final ConsumerInputEndpoint<E> output = new ConsumerInputEndpoint<>();
    private Containee<E, S> containee;
    private int makeSnapshotEveryMessageNo = -1;
    private int nextSnapshotInMessages = -1;

    public Container(String id, Function<String, Containee<E, S>> factory) {
        this(id, factory, null);
    }

    private Container(String id, Function<String, Containee<E, S>> factory,
                      StateManager<E, S> stateManager) {
        this.id = id;
        this.factory = factory;
        this.stateManager = new StateManager<>(this, stateManager);
    }

    public void makeSnapshotEveryMessageNo(int makeSnapshotEveryMessageNo) {
        this.makeSnapshotEveryMessageNo = makeSnapshotEveryMessageNo;
        nextSnapshotInMessages = makeSnapshotEveryMessageNo;
        sequencer.setOnInconsistent(v -> {
            if (sequencer.shouldRestore()) {
                stateManager.restart(id);
                log.info("Restored state: {}", getContainee());
            }
        });
    }

    public boolean isConsistent(long toSeq) {
        return sequencer.isConsistent(toSeq);
    }

    @Override
    public InputEndpoint<E> get() {
        return output;
    }

    @Override
    public OutputEndpoint<E> getInput() {
        return sequencer.getInput();
    }

    void replay(Iterator<Envelope<E>> envelope) {
        sequencer.reset();
        envelope.forEachRemaining(sequencer::send);
    }

    private void sendToContainee(Envelope<E> envelope) {
        containee.accept(envelope);
        if (stateManager.getState() == ContainerState.Running &&
                makeSnapshotEveryMessageNo > 0 && nextSnapshotInMessages-- == 0) {
            backupSnapshot();
            nextSnapshotInMessages = makeSnapshotEveryMessageNo;
        }
    }

    void setSnapshot(Snapshot<S> object) {
        containee.setSnapshot(object);
    }


    public <R extends Containee<E, S>> R getContainee() {
        return (R) containee;
    }

    public Snapshot<S> backupSnapshot() {
        Snapshot<S> snapshot = new Snapshot<>(sequencer.getLastSeq(), containee.getSnapshot());
        stateManager.set(snapshot);
        return snapshot;
    }

    public void setSnapshotConsumer(Consumer<Snapshot<S>> snapshotConsumer) {
        stateManager.setSnapshotConsumer(snapshotConsumer);
    }

    public void setSnapshotProducer(Supplier<Snapshot<S>> snapshotProducer) {
        stateManager.setSnapshotProducer(snapshotProducer);
    }

    public void setMessageReplay(Function<Long, Iterable<Envelope<E>>> messageReplay) {
        stateManager.setMessageReplay(messageReplay);
    }

    private void initContanee() {
        if (this.containee == null) this.containee = factory.apply(id);
    }

    public void start(String fromSnapshot) {
        initContanee();
        sequencer.get().subscribe(this::sendToContainee);
        containee.getContaineeOutput().subscribe(v -> {
            if (stateManager.getState() == ContainerState.Running) {
                output.send(v);
            } else {
                log.debug("Countainer is not started yet. Message hasn't passed out: {}", v);
            }
        });
        stateManager.start(fromSnapshot);
    }

    public Container<E, S> copy(String id) {
        return new Container<>(id, factory, stateManager);
    }

    public Container<E, S> start() {
        initContanee();
        start(containee.id);
        return this;
    }

    public void stop() {
        stateManager.stop();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Container<?, ?> that = (Container<?, ?>) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
