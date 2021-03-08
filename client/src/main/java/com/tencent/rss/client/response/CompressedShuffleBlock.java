package com.tencent.rss.client.response;

import java.nio.ByteBuffer;

public class CompressedShuffleBlock {

  private ByteBuffer byteBuffer;
  private int uncompressLength;

  public CompressedShuffleBlock(ByteBuffer byteBuffer, int uncompressLength) {
    this.byteBuffer = byteBuffer;
    this.uncompressLength = uncompressLength;
  }

  public int getUncompressLength() {
    return uncompressLength;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }
}
