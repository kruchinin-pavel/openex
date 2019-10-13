package org.openex.seda.services;

import org.openex.seda.message.Envelope;

public interface OutputEndpoint<T> {
    void send(Envelope<T> envelope);
}
