package org.openex.seda.services;

import org.openex.seda.message.Envelope;
import org.openex.seda.message.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public abstract class Containee<E, S> implements Consumer<Envelope<E>> {
    public final String id;
    private final ConsumerInputEndpoint<E> containeeOutput = new ConsumerInputEndpoint<>();
    private final List<E> replies = new ArrayList<>();
    private final Logger log;

    protected Containee(String id) {
        this.id = id;
        log = LoggerFactory.getLogger(Containee.class + ":" + id);
    }

    ConsumerInputEndpoint<E> getContaineeOutput() {
        return containeeOutput;
    }

    @Override
    public void accept(Envelope<E> envelope) {
        implProcess(envelope);
        containeeOutput.send(new Envelope<>(envelope.seq, new ArrayList<>(this.replies)));
        replies.clear();
    }

    protected abstract void implProcess(Envelope<E> envelope);

    public abstract S getSnapshot();

    public abstract void setSnapshot(Snapshot<S> object);

    protected final void send(E event) {
        replies.add(event);
    }

}
