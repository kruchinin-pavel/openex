package org.openex.seda.services;

import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;

public interface OutputGetter<T> {
    InputEndpoint<T> get();

    default Envelope<T> poll() {
        return get().poll();
    }
}
