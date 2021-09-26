package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.tencent.rss.common.util.ExitUtils;
import com.tencent.rss.common.util.ExitUtils.ExitException;
import com.tencent.rss.storage.util.StorageType;
import org.junit.Test;

public class ShuffleServerTest {

  @Test
  public void startTest() {
    try {
      ShuffleServerConf serverConf = new ShuffleServerConf();
      serverConf.setInteger("rss.rpc.server.port", 9527);
      serverConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
      serverConf.setInteger("rss.jetty.http.port", 9528);
      serverConf.setString("rss.coordinator.quorum", "localhost:0");
      serverConf.setString("rss.storage.basePath", "/dev/null");
      serverConf.setString("rss.server.buffer.capacity", "100");
      serverConf.setString("rss.server.read.buffer.capacity", "10");
      serverConf.setString("rss.server.partition.buffer.size", "5");
      serverConf.setString("rss.server.buffer.spill.threshold", "2");

      ShuffleServer ss1 = new ShuffleServer(serverConf);
      ss1.start();

      ExitUtils.disableSystemExit();
      ShuffleServer ss2 = new ShuffleServer(serverConf);
      String expectMessage = "Fail to start jetty http server";
      final int expectStatus = 1;
      try {
        ss2.start();
      } catch (Exception e) {
        assertEquals(expectMessage, e.getMessage());
        assertEquals(expectStatus, ((ExitException) e).getStatus());
      }

      serverConf.setInteger("rss.jetty.http.port", 9529);
      ss2 = new ShuffleServer(serverConf);
      expectMessage = "Fail to start grpc server";
      try {
        ss2.start();
      } catch (Exception e) {
        assertEquals(expectMessage, e.getMessage());
        assertEquals(expectStatus, ((ExitException) e).getStatus());
      }

      final Thread t = new Thread(null, () -> {
        throw new AssertionError("TestUncaughtException");
      }, "testThread");
      t.start();
      t.join();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

  }
}
