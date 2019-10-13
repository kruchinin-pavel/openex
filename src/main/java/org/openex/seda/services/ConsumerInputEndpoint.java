package org.openex.seda.services;

import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.Consumer;

public class ConsumerInputEndpoint<T> implements InputEndpoint<T> {
    private Collection<Consumer<Envelope<T>>> receiver = Collections.emptyList();

    @Override
    public boolean subscribe(Consumer<Envelope<T>> envelopeConsumer) {
        if (envelopeConsumer == null) throw new NullPointerException();
        if (receiver.size() == 0) {
            receiver = Collections.singleton(envelopeConsumer);
            return true;
        } else if (receiver.size() == 1) {
            receiver = new HashSet<>(receiver);
        }
        return receiver.add(envelopeConsumer);
    }

    public void send(Envelope<T> msg) {
        receiver.forEach(v -> v.accept(msg));
    }
}
