package org.openex.orderbook.stress;

import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;
import org.openex.seda.services.ConsumerInputEndpoint;
import org.openex.seda.services.InputGetter;
import org.openex.seda.services.OutputEndpoint;
import org.openex.seda.services.OutputGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class MessageLooser<T> implements InputGetter<T>, OutputGetter<T> {
    private static final Logger log = LoggerFactory.getLogger(MessageLooser.class);
    private final ConsumerInputEndpoint<T> output = new ConsumerInputEndpoint<>();
    long looseMessageSeq = -1;
    double messageLostProbability;

    MessageLooser() {
    }

    @Override
    public InputEndpoint<T> get() {
        return output;
    }

    @Override
    public OutputEndpoint<T> getInput() {
        return this::send;
    }

    private void send(Envelope<T> msg) {
        boolean looseNextMessage = looseMessageSeq == msg.seq ||
                ThreadLocalRandom.current().nextDouble() < messageLostProbability;
        if (!looseNextMessage) {
            output.send(msg);
        } else {
            log.info("Loosing message: {}", msg);
        }
    }
}
