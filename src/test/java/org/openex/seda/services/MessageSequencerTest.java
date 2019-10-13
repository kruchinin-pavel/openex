package org.openex.seda.services;

import org.junit.Test;
import org.openex.seda.message.Envelope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class MessageSequencerTest {
    @Test
    public void doTest() {
        List<Integer> sorted = new ArrayList<>();
        List<Integer> source = new ArrayList<>();

        MessageSequencer<Integer> sorter = new MessageSequencer<>();
        sorter.get().subscribe(msg -> sorted.add(msg.getPayload()));

        for (int i = 2; i <= 100; i++) source.add(i);
        Collections.shuffle(source);
        source.add(0, 1);

        source.forEach(i -> sorter.send(new Envelope<>(i, i)));

        assertEquals(source.size(), sorted.size());
        int i = sorted.get(0);
        for (int val : sorted) assertEquals(i++, val);
    }

    @Test
    public void testDuplicates() {
        PlainMemorySedaFactory<Integer, Integer> factory = PlainMemorySedaFactory.create();
        MessageEnveloper<Integer> rcv = factory.getEnveloper();
        MessageSequencer<Integer> sequencer = factory.sequencer();

        LinkedList<Envelope<Integer>> draftMessages = new LinkedList<>();
        LinkedList<Envelope<Integer>> filteredMessages = new LinkedList<>();
        factory.connect(rcv, sequencer);
        factory.connect(rcv, () -> draftMessages::addLast);
        factory.connect(sequencer, () -> filteredMessages::addLast);

        for (int i = 0; i < 10; i++) rcv.send(i);

        draftMessages.forEach(sequencer::send);

        assertEquals(10, draftMessages.size());
        assertEquals(10, filteredMessages.size());
    }

    @Test
    public void testInconsistent() {
        PlainMemorySedaFactory<Integer, Integer> factory = PlainMemorySedaFactory.create();
        MessageSequencer<Integer> sequencer = factory.sequencer();

        sequencer.send(new Envelope<>(1));
        assertTrue(sequencer.isConsistent());
        sequencer.send(new Envelope<>(3));
        assertFalse(sequencer.isConsistent());
        sequencer.send(new Envelope<>(2));
        assertTrue(sequencer.isConsistent());
    }

}