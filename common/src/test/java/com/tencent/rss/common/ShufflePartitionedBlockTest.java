package com.tencent.rss.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ShufflePartitionedBlockTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shufflePartitionedBlockTest() {
        ShufflePartitionedBlock b1 = new ShufflePartitionedBlock(1, 2, 3);
        assertEquals(1, b1.getLength());
        assertEquals(2, b1.getCrc());
        assertEquals(3, b1.getBlockId());

        byte[] buf = new byte[3];
        new Random().nextBytes(buf);

        ByteBuffer bb = ByteBuffer.wrap(buf);
        ShufflePartitionedBlock b2 = new ShufflePartitionedBlock(1, 2, 3, bb);
        assertEquals(bb, b2.getData());
        assertArrayEquals(buf, b2.getData().array());

        ShufflePartitionedBlock b3 = new ShufflePartitionedBlock(1, 2, 3, buf);
        assertArrayEquals(buf, b3.getData().array());

        ByteBuffer nullBb = null;
        thrown.expect(NullPointerException.class);
        new ShufflePartitionedBlock(1, 2, 3, nullBb);

        byte[] nullBuf = null;
        thrown.expect(NullPointerException.class);
        new ShufflePartitionedBlock(1, 2, 3, nullBuf);

    }
}
