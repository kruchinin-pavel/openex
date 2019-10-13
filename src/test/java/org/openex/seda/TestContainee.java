package org.openex.seda;

import org.openex.seda.message.Envelope;
import org.openex.seda.message.Snapshot;
import org.openex.seda.services.Containee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

class TestContainee extends Containee<Integer, String> {
    private final Logger logger = LoggerFactory.getLogger(TestContainee.class);
    private int currentVal;
    private long lastSeq = -1;

    TestContainee(String id) {
        super(id);
    }

    int getCurrentVal() {
        return currentVal;
    }

    @Override
    protected void implProcess(Envelope<Integer> envelope) {
        if (lastSeq >= envelope.seq) return;
        currentVal++;
        logger.info("{} got event: {}", id, envelope.payload);
        lastSeq = envelope.seq;
    }

    @Override
    public String getSnapshot() {
        return "snp_" + currentVal;
    }

    @Override
    public void setSnapshot(Snapshot<String> snapshot) {
        if (snapshot == null) {
            currentVal = 0;
        } else {
            currentVal = Integer.parseInt(snapshot.payload.substring("snp_".length()));
            lastSeq = snapshot.seq;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestContainee that = (TestContainee) o;
        return currentVal == that.currentVal &&
                lastSeq == that.lastSeq;
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentVal, lastSeq);
    }

    @Override
    public String toString() {
        return "TestContainee{" +
                "currentVal=" + currentVal +
                ", lastSeq=" + lastSeq +
                '}';
    }
}
