package org.openex.orderbook.stress;

import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;
import org.openex.seda.services.ConsumerInputEndpoint;
import org.openex.seda.services.InputGetter;
import org.openex.seda.services.OutputEndpoint;
import org.openex.seda.services.OutputGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class MessageLooser<T> implements InputGetter<T>, OutputGetter<T> {
    private static final Logger log = LoggerFactory.getLogger(MessageLooser.class);
    private final ConsumerInputEndpoint<T> output = new ConsumerInputEndpoint<>();

    MessageLooser() {
    }

    private Function<Envelope<T>, Boolean> loseFunction = v -> false;

    public MessageLooser<T> setLoseFunction(Function<Envelope<T>, Boolean> loseFunction) {
        this.loseFunction = loseFunction;
        return this;
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
        if (!loseFunction.apply(msg)) {
            output.send(msg);
        } else {
            log.info("Loosing message: {}", msg);
        }
    }
}
