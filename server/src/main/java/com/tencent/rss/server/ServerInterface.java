package com.tencent.rss.server;

import java.io.IOException;

public interface ServerInterface {

  void start() throws IOException;

  void stop() throws InterruptedException;

  void blockUntilShutdown() throws InterruptedException;
}
