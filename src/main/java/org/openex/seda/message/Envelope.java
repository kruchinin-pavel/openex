package org.openex.seda.message;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class Envelope<T> {
    public final long seq;
    public final List<T> payload;

    public Envelope(long seq) {
        this.seq = seq;
        this.payload = Collections.emptyList();
    }

    public Envelope(long seq, List<T> payload) {
        this(0, seq, payload, -1);
    }

    private Envelope(long channel, long seq, List<T> payload, long inReplyToRequestId) {
        this.seq = seq;
        this.payload = payload;
    }

    public Envelope(long seq, T payload) {
        this(seq, Collections.singletonList(payload));
    }

    public boolean isEmpty() {
        return payload.size() == 0;
    }

    public void forEach(Consumer<T> consumer) {
        payload.forEach(consumer);
    }


    public Envelope<T> inReply(long newSeq) {
        return new Envelope<>(-1, newSeq, payload, seq);
    }

    public List<T> asList() {
        return payload;
    }

    public <R extends T> R getPayload() {
        return (R) payload.get(0);
    }

    @Override
    public String toString() {
        return "Message{" +
                "sequenceId=" + seq +
                ", payload=" + payload +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Envelope<?> envelope = (Envelope<?>) o;
        return seq == envelope.seq &&
                Objects.equals(payload, envelope.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seq, payload);
    }
}
