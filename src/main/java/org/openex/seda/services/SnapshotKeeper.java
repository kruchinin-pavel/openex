package org.openex.seda.services;

import org.openex.seda.message.Snapshot;

public class SnapshotKeeper {
    private Snapshot snapshot;

    void putSnapshot(Snapshot value) {
        snapshot = value;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

}
