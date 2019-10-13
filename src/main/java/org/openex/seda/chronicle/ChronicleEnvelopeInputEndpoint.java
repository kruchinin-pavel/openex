package org.openex.seda.chronicle;

import net.openhft.chronicle.bytes.BytesIn;
import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ChronicleEnvelopeInputEndpoint<T> extends ChronicleInputEndpoint<Envelope<T>>
        implements InputEndpoint<T>, AutoCloseable {

    public ChronicleEnvelopeInputEndpoint(String path, Function<BytesIn, T> reader) {
        super(path, v -> {
            long seq = v.readLong();
            int cnt = v.readInt();
            List<T> vals = new ArrayList<>(cnt);
            for (int i = 0; i < cnt; i++) {
                vals.add(reader.apply(v));
            }
            return new Envelope<>(seq, vals);
        });
    }

}
