package org.openex.orderbook.stress;

import net.openhft.chronicle.bytes.BytesIn;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openex.seda.AbstractSedaFactory;
import org.openex.seda.chronicle.ChronicleSedaFactory;
import org.openex.seda.services.MessageEnveloper;
import org.openex.seda.services.MessageSequencer;
import org.openex.seda.services.PlainMemorySedaFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MessageLooserTest {
    private final AbstractSedaFactory<Integer, Integer> factory;

    public MessageLooserTest(AbstractSedaFactory<Integer, Integer> factory) {
        this.factory = factory;
    }

    @Parameterized.Parameters
    public static List<Object[]> params() {
        String eventPath = "build/MessageLooserTest_evt";
        String snapPath = "build/MessageLooserTest_snap";
        ChronicleSedaFactory.clearPath(eventPath);
        ChronicleSedaFactory.clearPath(snapPath);
        return Arrays.asList(
                new Object[]{PlainMemorySedaFactory.create()},
                new Object[]{ChronicleSedaFactory.create(
                        eventPath,
                        snapPath,
                        (v, out) -> out.writeInt(v), BytesIn::readInt,
                        (v, out) -> out.writeInt(v), BytesIn::readInt)
                }
        );
    }

    @Test
    public void test() {
        MessageEnveloper<Integer> rcv = factory.getEnveloper();
        MessageLooser<Integer> loser = new MessageLooser<>();
        loser.messageLostProbability = 0.05;
        MessageSequencer<Integer> seq = factory.sequencer();
        factory.connect(rcv, loser);
        factory.connect(loser, seq);
        for (int i = 0; i < 1_000; i++) rcv.send(i);
        assertFalse(seq.isConsistent());
        assertTrue(seq.shouldRestore());

    }

}