package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ShuffleBufferTest {

    private ShuffleBuffer shuffleBuffer = new ShuffleBuffer(128, 300, 1, 200);

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {
        shuffleBuffer.clear();
    }

    @Test
    public void appendAndGetTest() {
        List<ShufflePartitionedData> shuffleData = new LinkedList<>();
        ShufflePartitionedBlock shuffleBlock1 = new ShufflePartitionedBlock(10, 1, 1);
        ShufflePartitionedBlock shuffleBlock2 = new ShufflePartitionedBlock(20, 3, 3);
        ShufflePartitionedBlock shuffleBlock3 = new ShufflePartitionedBlock(30, 5, 5);

        shuffleData.add(new ShufflePartitionedData(1, shuffleBlock1));
        shuffleData.add(new ShufflePartitionedData(100, shuffleBlock2));
        shuffleData.add(new ShufflePartitionedData(23, shuffleBlock3));

        shuffleData.forEach(d -> shuffleBuffer.append(d));
        //List<ShuffleBlock> a = shuffleBuffer.getBlocks(1);

        assertEquals(1, shuffleBuffer.getBlocks(1).size());
        assertEquals(1, shuffleBuffer.getBlocks(100).size());
        assertEquals(1, shuffleBuffer.getBlocks(23).size());

        assertEquals(1, shuffleBuffer.getBlocks(1).get(0).getBlockId());
        assertEquals(3, shuffleBuffer.getBlocks(100).get(0).getBlockId());
        assertEquals(5, shuffleBuffer.getBlocks(23).get(0).getBlockId());

        int expected = (10 + 20 + 30) + 20 * 3;
        assertEquals(expected, shuffleBuffer.getSize());
        assertFalse(shuffleBuffer.full());

        ShufflePartitionedBlock shuffleBlock4 = new ShufflePartitionedBlock(10, 25, 25);
        shuffleBuffer.append(new ShufflePartitionedData(23, shuffleBlock4));
        assertTrue(shuffleBuffer.full());
        assertEquals(2, shuffleBuffer.getBlocks(23).size());
        assertEquals(25, shuffleBuffer.getBlocks(23).get(1).getBlockId());

    }

}
