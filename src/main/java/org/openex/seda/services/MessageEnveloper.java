package org.openex.seda.services;

import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.interfaces.Receiver;
import org.openex.seda.message.Envelope;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class MessageEnveloper<T> implements Receiver<T>, OutputGetter<T>, InputGetter<T> {
    private final Supplier<Long> sequencer = new AtomicLong()::incrementAndGet;
    private final ConsumerInputEndpoint<T> enveloperOutput = new ConsumerInputEndpoint<>();
    private final OutputEndpoint<T> input = msg -> enveloperOutput.send(msg.inReply(sequencer.get()));

    @Override
    public AtomicReference<Envelope<T>> send(T message) {
        Envelope<T> letter = new Envelope<>(sequencer.get(), message);
        enveloperOutput.send(letter);
        return new AtomicReference<>(letter);
    }

    @Override
    public InputEndpoint<T> get() {
        return enveloperOutput;
    }

    @Override
    public OutputEndpoint<T> getInput() {
        return input;
    }
}
