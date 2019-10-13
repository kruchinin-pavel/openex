package org.openex.seda.chronicle;

import net.openhft.chronicle.bytes.BytesOut;
import org.openex.seda.message.Envelope;
import org.openex.seda.services.OutputEndpoint;

import java.util.function.BiConsumer;

public class ChronicleEnvelopeOutputEndpoint<T> extends ChronicleOutputEndpoint<Envelope<T>>
        implements OutputEndpoint<T>, AutoCloseable {

    public ChronicleEnvelopeOutputEndpoint(String path, BiConsumer<T, BytesOut> serializer) {
        super(path, (env, o) -> {
            o.writeLong(env.seq);
            o.writeInt(env.asList().size());
            for (T v : env.asList()) {
                serializer.accept(v, o);
            }
        });
    }

}
