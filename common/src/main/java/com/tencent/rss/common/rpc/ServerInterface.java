package com.tencent.rss.common.rpc;

import java.io.IOException;

public interface ServerInterface {

  void start() throws IOException;

  void stop() throws InterruptedException;

  void blockUntilShutdown() throws InterruptedException;
}
