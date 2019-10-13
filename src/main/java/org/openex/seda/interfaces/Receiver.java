package org.openex.seda.interfaces;

import org.openex.seda.message.Envelope;

import java.util.concurrent.atomic.AtomicReference;

public interface Receiver<T> {
    AtomicReference<Envelope<T>> send(T message);
}
