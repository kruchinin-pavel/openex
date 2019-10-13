package org.openex.seda.services;

public interface InputGetter<T> {
    OutputEndpoint<T> getInput();
}
