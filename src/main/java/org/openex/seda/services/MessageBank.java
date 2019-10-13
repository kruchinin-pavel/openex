package org.openex.seda.services;

import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;

import java.util.LinkedList;
import java.util.ListIterator;

public class MessageBank<T> implements InputGetter<T>, OutputGetter<T> {
    private final LinkedList<Envelope<T>> messages = new LinkedList<>();
    private final MessageSequencer<T> sequencer = new MessageSequencer<>();
    private final ConsumerInputEndpoint<T> output = new ConsumerInputEndpoint<>();

    public MessageBank() {
        sequencer.get().subscribe(messages::addLast);
    }

    @Override
    public OutputEndpoint<T> getInput() {
        return sequencer.getInput();
    }

    @Override
    public InputEndpoint<T> get() {
        return output;
    }

    public Iterable<Envelope<T>> replay(Long seq) {
        return () -> {
            ListIterator<Envelope<T>> iter = messages.listIterator(0);
            while (iter.hasNext()) {
                Envelope<T> env = iter.next();
                if (env.seq > seq) {
                    iter.previous();
                    break;
                }
            }
            return iter;
        };
    }

    void truncateBySeq(long seq) {
        Envelope first;
        while ((first = messages.peekFirst()) != null && first.seq < seq) {
            messages.removeFirst();
        }
    }
}