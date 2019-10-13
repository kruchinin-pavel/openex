package org.openex.seda.interfaces;

import org.openex.seda.message.Envelope;

import java.util.function.Consumer;

public interface InputEndpoint<T> {
    boolean subscribe(Consumer<Envelope<T>> consumer);

    default Envelope<T> pollLast() {
        Envelope<T> last;
        while ((last = poll()) != null) ;
        return last;
    }

    default Envelope<T> poll() {
        return null;
    }
}
