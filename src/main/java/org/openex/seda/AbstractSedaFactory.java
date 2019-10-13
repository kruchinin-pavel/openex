package org.openex.seda;

import org.jetbrains.annotations.NotNull;
import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;
import org.openex.seda.message.Snapshot;
import org.openex.seda.services.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractSedaFactory<E, S> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractSedaFactory.class);
    private final MessageEnveloper<E> enveloper = new MessageEnveloper<>();

    public MessageEnveloper<E> getEnveloper() {
        return enveloper;
    }

    /**
     * Messages being input to factory
     *
     * @return getter for endpoint
     */
    public OutputGetter<E> input() {
        return this::getInput;
    }

    protected abstract InputEndpoint<E> getInput();

    public void connect(OutputGetter<E> fromOutput, InputGetter<E> toInput) {
        connect(fromOutput.get(), toInput.getInput());
    }

    protected void connect(InputEndpoint<E> fromOutput, OutputEndpoint<E> toInput) {
        fromOutput.subscribe(toInput::send);
    }

    public MessageSequencer<E> sequencer() {
        return new MessageSequencer<>();
    }

    public Container<E, S> container(String id, Function<String, Containee<E, S>> factory) {
        Container<E, S> container = new Container<>(id, factory);
        container.setSnapshotConsumer(getSnapshotStorage());
        container.setSnapshotProducer(getSnapshotSource());
        container.setMessageReplay(getMessageReplayer());
        return container;
    }

    @NotNull
    protected abstract Supplier<Snapshot<S>> getSnapshotSource();

    @NotNull
    protected abstract Function<Long, Iterable<Envelope<E>>> getMessageReplayer();

    @NotNull
    protected abstract Consumer<Snapshot<S>> getSnapshotStorage();

    public abstract ExecutorService pollInBackground();

}