package org.openex.seda.message;

import java.util.Objects;

public class Snapshot<T> {
    public final long seq;
    public final T payload;

    public Snapshot(long seq, T payload) {
        this.seq = seq;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Snapshot{" +
                "sequence=" + seq +
                ", payload=" + payload +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Snapshot<?> snapshot = (Snapshot<?>) o;
        return seq == snapshot.seq &&
                Objects.equals(payload, snapshot.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seq, payload);
    }
}
