package org.openex.seda.chronicle;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openex.seda.message.Envelope;
import org.openex.seda.services.MessageEnveloper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ChronicleTest {
    private static final Logger logger = LoggerFactory.getLogger(ChronicleTest.class);
    private static final AtomicInteger stat_count = new AtomicInteger();
    private final List<String> expected = new ArrayList<>();
    private final List<String> restored = new ArrayList<>();
    private final AtomicInteger counter = new AtomicInteger(100);
    private final String PATH_STR = "build/trades_" + stat_count.incrementAndGet();
    private final String PATH_STR2 = "build/snaps_" + stat_count.incrementAndGet();

    @Before
    public void prepare() {
        for (int i = 0; i < 10; i++) expected.add("String: " + i);
        ChronicleSedaFactory.clearPath(PATH_STR);
        ChronicleSedaFactory.clearPath(PATH_STR2);
    }

    @Test
    public void testChronicleBytes() {
        List<Object> vals = new ArrayList<>();
        ChronicleQueue queue = ChronicleQueue.singleBuilder(PATH_STR).build();
        final ExcerptAppender appender = queue.acquireAppender();
        expected.forEach(v -> {
            try (final DocumentContext dc = appender.writingDocument()) {
                requireNonNull(dc.wire()).writeBytes(o -> {
                    o.writeInt(counter.incrementAndGet());
                    o.writeLong(counter.incrementAndGet());
                    o.writeUtf8(v);
                });
            }
        });

        final ExcerptTailer tailer = queue.createTailer();
        while (tailer.peekDocument()) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.wire() == null) continue;
                requireNonNull(dc.wire()).readBytes(i -> {
                    int v1 = i.readInt();
                    long v2 = i.readLong();
                    String v3 = i.readUtf8();
                    logger.info("v1={}, v2={}, v3={}", v1, v2, v3);
                    restored.add(v3);
                });
            }
        }
        compare();
    }

    @Test
    public void testChronicleEndpoints() {
        final ChronicleOutputEndpoint<String> out = new ChronicleOutputEndpoint<>(PATH_STR, (v, o) -> {
            o.writeInt(counter.incrementAndGet());
            o.writeLong(counter.incrementAndGet());
            o.writeUtf8(v);
        });
        expected.forEach(out::send);

        final ChronicleInputEndpoint<String> in = new ChronicleInputEndpoint<>(PATH_STR, valueIn -> {
            valueIn.readInt();
            valueIn.readLong();
            return valueIn.readUtf8();
        });
        in.subscribe(restored::add);

        while (in.poll() != null) ;
        compare(in.getLastEvent());
    }

    @Test
    public void testChronicleEnvelopedEndpoints() {
        List<Envelope<String>> expectedEnvelopes = new ArrayList<>();
        List<Envelope<String>> restoredEnvelopes = new ArrayList<>();
        final ChronicleEnvelopeOutputEndpoint<String> out =
                new ChronicleEnvelopeOutputEndpoint<>(PATH_STR, (v, o) -> o.writeUtf8(v));
        expected.forEach(v -> {
            Envelope<String> event = new Envelope<>(counter.incrementAndGet(), v);
            expectedEnvelopes.add(event);
            out.send(event);
        });

        final ChronicleEnvelopeInputEndpoint<String> in = new ChronicleEnvelopeInputEndpoint<>(PATH_STR, BytesIn::readUtf8);
        in.subscribe(env -> {
            restoredEnvelopes.add(env);
            restored.add(env.getPayload());
        });

        while (in.poll() != null) ;
        compare(in.getLastEvent().getPayload());
        Assert.assertEquals(expectedEnvelopes, restoredEnvelopes);
    }


    @Test
    public void testChronicleFactory() {
        ChronicleSedaFactory<String, String> factory = ChronicleSedaFactory.create(PATH_STR, null,
                (v, out) -> out.writeUtf8(v), BytesIn::readUtf8, null, null);

        MessageEnveloper<String> out = factory.getEnveloper();
        for (String str : expected) out.send(str);

        ChronicleEnvelopeInputEndpoint<String> in = factory.getInput();
        in.subscribe(env -> restored.add(env.getPayload()));

        while (in.poll() != null) ;

        compare();
    }


    private void compare() {
        assertTrue(expected.size() > 0);
        assertEquals(expected, restored);
    }

    private void compare(String lastEvent) {
        compare();
        assertEquals(expected.get(expected.size() - 1), lastEvent);
    }

}
