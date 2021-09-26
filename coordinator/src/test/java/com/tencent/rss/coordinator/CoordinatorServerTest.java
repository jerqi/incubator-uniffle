package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.tencent.rss.common.util.ExitUtils;
import com.tencent.rss.common.util.ExitUtils.ExitException;
import org.junit.Test;

public class CoordinatorServerTest {

  @Test
  public void test() {
    try {
      CoordinatorConf coordinatorConf = new CoordinatorConf();
      coordinatorConf.setInteger("rss.rpc.server.port", 9537);
      coordinatorConf.setInteger("rss.jetty.http.port", 9528);
      coordinatorConf.setInteger("rss.rpc.executor.size", 10);

      CoordinatorServer cs1 = new CoordinatorServer(coordinatorConf);
      CoordinatorServer cs2 = new CoordinatorServer(coordinatorConf);
      cs1.start();

      ExitUtils.disableSystemExit();
      String expectMessage = "Fail to start jetty http server";
      final int expectStatus = 1;
      try {
        cs2.start();
      } catch (Exception e) {
        assertEquals(expectMessage, e.getMessage());
        assertEquals(expectStatus, ((ExitException) e).getStatus());
      }

      coordinatorConf.setInteger("rss.jetty.http.port", 9529);
      cs2 = new CoordinatorServer(coordinatorConf);
      expectMessage = "Fail to start grpc server";
      try {
        cs2.start();
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
