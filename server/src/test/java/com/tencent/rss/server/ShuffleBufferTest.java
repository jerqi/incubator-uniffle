package com.tencent.rss.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import com.tencent.rss.proto.RssProtos.ShuffleData;
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
        List<ShuffleData> shuffleData = new LinkedList<>();
        ShuffleBlock shuffleBlock1 = ShuffleBlock.newBuilder().setBlockId(1L).setLength(10).build();
        ShuffleBlock shuffleBlock2 = ShuffleBlock.newBuilder().setBlockId(3L).setLength(20).build();
        ShuffleBlock shuffleBlock3 = ShuffleBlock.newBuilder().setBlockId(5L).setLength(30).build();

        shuffleData.add(ShuffleData.newBuilder().setPartitionId(1).addBlock(shuffleBlock1).build());
        shuffleData.add(ShuffleData.newBuilder().setPartitionId(100).addBlock(shuffleBlock2).build());
        shuffleData.add(ShuffleData.newBuilder().setPartitionId(23).addBlock(shuffleBlock3).build());

        shuffleData.forEach(d -> shuffleBuffer.append(d));
        List<ShuffleBlock> a = shuffleBuffer.getBlocks(1);

        assertEquals(1, shuffleBuffer.getBlocks(1).size());
        assertEquals(1, shuffleBuffer.getBlocks(100).size());
        assertEquals(1, shuffleBuffer.getBlocks(23).size());

        assertEquals(1, shuffleBuffer.getBlocks(1).get(0).getBlockId());
        assertEquals(3, shuffleBuffer.getBlocks(100).get(0).getBlockId());
        assertEquals(5, shuffleBuffer.getBlocks(23).get(0).getBlockId());

        int expected = (10 + 20 + 30) + 20 * 3;
        assertEquals(expected, shuffleBuffer.getSize());
        assertFalse(shuffleBuffer.full());

        ShuffleBlock shuffleBlock4 = ShuffleBlock.newBuilder().setBlockId(25L).setLength(10).build();
        shuffleBuffer.append(ShuffleData.newBuilder().addBlock(shuffleBlock4).setPartitionId(23).build());
        assertTrue(shuffleBuffer.full());
        assertEquals(2, shuffleBuffer.getBlocks(23).size());
        assertEquals(25, shuffleBuffer.getBlocks(23).get(1).getBlockId());

    }

}
