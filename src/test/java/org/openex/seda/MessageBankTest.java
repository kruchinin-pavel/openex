package org.openex.seda;

import org.junit.Test;
import org.openex.seda.message.Envelope;
import org.openex.seda.services.MessageBank;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class MessageBankTest {
    @Test
    public void doTest() {
        MessageBank<Integer> bank = new MessageBank<>();
        for (int i = 0; i < 10; i++) bank.getInput().send(new Envelope<>(i, i));
        Iterator<Envelope<Integer>> iterator = bank.replay(5L).iterator();
        Envelope<Integer> next = iterator.next();
        assertEquals((Integer) 6, next.getPayload());
    }

}