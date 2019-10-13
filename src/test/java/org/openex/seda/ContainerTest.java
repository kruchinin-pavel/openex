package org.openex.seda;

import net.openhft.chronicle.bytes.BytesIn;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openex.seda.chronicle.ChronicleSedaFactory;
import org.openex.seda.interfaces.InputEndpoint;
import org.openex.seda.message.Envelope;
import org.openex.seda.message.Snapshot;
import org.openex.seda.services.Container;
import org.openex.seda.services.MessageEnveloper;
import org.openex.seda.services.PlainMemorySedaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ContainerTest {
    private static final AtomicInteger testCounter = new AtomicInteger();
    private static final int MAX_VAL = 10;
    private static final Logger log = LoggerFactory.getLogger(ContainerTest.class);
    private final AbstractSedaFactory<Integer, String> factory;

    public ContainerTest(AbstractSedaFactory<Integer, String> factory) {
        this.factory = factory;
    }

    @Parameterized.Parameters
    public static List<Object[]> params() {
        int count = testCounter.incrementAndGet();
        String eventPath = "build/ContainerTestChronicle_" + count;
        String snapPath = "build/ContainerTestChronicleSnapshot_" + count;
        ChronicleSedaFactory.clearPath(eventPath);
        ChronicleSedaFactory.clearPath(snapPath);
        return Arrays.asList(
                new Object[]{PlainMemorySedaFactory.create()},
                new Object[]{ChronicleSedaFactory.create(
                        eventPath,
                        snapPath,
                        (v, out) -> out.writeInt(v),
                        BytesIn::readInt,
                        (v, out) -> out.writeUtf8(v),
                        BytesIn::readUtf8)
                }
        );
    }

    @Test
    public void testSnapshot() {
        MessageEnveloper<Integer> enveloper = factory.getEnveloper();
        Container<Integer, String> cntn1 = factory.container("1", TestContainee::new).start();
        factory.connect(factory.input(), cntn1);

        // Send half of messages
        for (int i = 0; i < MAX_VAL; i++) enveloper.send(i);
        factory.getInput().pollLast();

        Snapshot<String> snapshot = cntn1.backupSnapshot();
        Snapshot<String> restoredSnapshot = factory.getSnapshotSource().get();
        assertEquals(snapshot, restoredSnapshot);
    }


    @Test
    public void test() {
        MessageEnveloper<Integer> enveloper = factory.getEnveloper();
        Container<Integer, String> cntn1 = factory.container("1", TestContainee::new).start();
        Container<Integer, String> cntn3 = factory.container("3", TestContainee::new).start();
        factory.connect(enveloper, cntn1);
        factory.connect(factory.input(), cntn3);

        // Send half of messages
        for (int i = 0; i < MAX_VAL / 2; i++) enveloper.send(ThreadLocalRandom.current().nextInt());

        InputEndpoint<Integer> ep = factory.input().get();
        Envelope<Integer> lval;
        while ((lval = ep.poll()) != null) {
            log.info("Input envelope come: {} ", lval);
        }

        assertEquals(((TestContainee) cntn1.getContainee()).getCurrentVal(),
                ((TestContainee) cntn3.getContainee()).getCurrentVal());

        cntn1.backupSnapshot();

        // Send another half of messages
        for (int i = MAX_VAL / 2; i < MAX_VAL; i++) enveloper.send(i);

        assertEquals(MAX_VAL, ((TestContainee) cntn1.getContainee()).getCurrentVal());
        Assert.assertEquals("Snapshot should be a half part of MAX_VAL", "snp_" + MAX_VAL / 2,
                factory.getSnapshotSource().get().payload);

        Container<Integer, String> restored = cntn1.copy("2");
        factory.connect(enveloper, restored);
        restored.start("1");

        assertEquals(cntn1.getContainee(), restored.getContainee());
    }

}