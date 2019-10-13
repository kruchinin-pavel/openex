package org.openex.seda.services;

import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.function.Consumer;

public class MessageSequencer<T> implements OutputEndpoint<T>, InputGetter<T>, OutputGetter<T> {
    private final TreeSet<Envelope<T>> msgSet = new TreeSet<>(Comparator.comparingLong(o -> o.seq));
    private final ConsumerInputEndpoint<T> sequencerOutput = new ConsumerInputEndpoint<>();
    private long lastSeq = -1;
    private long firstSeq = -1;
    private Consumer<Boolean> onInconsistent = null;

    @Override
    public InputEndpoint<T> get() {
        return sequencerOutput;
    }

    @Override
    public OutputEndpoint<T> getInput() {
        return this;
    }

    void setOnInconsistent(Consumer<Boolean> onInconsistent) {
        this.onInconsistent = onInconsistent;
    }

    @Override
    public void send(Envelope<T> envelope) {
        if (firstSeq > envelope.seq || msgSet.contains(envelope)) return;
        msgSet.add(envelope);
        if (lastSeq < envelope.seq) {
            lastSeq = envelope.seq;
        }
        if (envelope.seq < firstSeq || firstSeq == -1) {
            firstSeq = envelope.seq;
        }

        long seq = firstSeq;
        Iterator<Envelope<T>> iter = msgSet.iterator();
        while (iter.hasNext()) {
            Envelope<T> cur = iter.next();
            if (cur.seq != seq++) break;
            sequencerOutput.send(cur);
            iter.remove();
            firstSeq = seq;
        }
        if (!isConsistent() && onInconsistent != null) {
            onInconsistent.accept(false);
        }
    }

    public long getLastSeq() {
        return lastSeq;
    }

    public boolean isConsistent() {
        return msgSet.size() == 0;
    }

    public boolean shouldRestore() {
        return msgSet.size() > 5;
    }

    void reset() {
        msgSet.clear();
        firstSeq = lastSeq = -1;
    }

    boolean isConsistent(long toSeq) {
        return lastSeq == toSeq && msgSet.size() == 0;
    }

}
