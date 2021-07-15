package com.tencent.rss.client.response;

import com.tencent.rss.common.util.RssUtils;
import java.io.IOException;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RssGetShuffleResultResponse extends ClientResponse {

  private Roaring64NavigableMap blockIdBitmap;

  public RssGetShuffleResultResponse(ResponseStatusCode statusCode, byte[] serializedBitmap) throws IOException {
    super(statusCode);
    blockIdBitmap = RssUtils.deserializeBitMap(serializedBitmap);
  }

  public Roaring64NavigableMap getBlockIdBitmap() {
    return blockIdBitmap;
  }
}
