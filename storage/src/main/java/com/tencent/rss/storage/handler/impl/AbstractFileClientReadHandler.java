package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.handler.api.ClientReadHandler;

public abstract class AbstractFileClientReadHandler implements ClientReadHandler {

  protected String appId;
  protected int shuffleId;
  protected int partitionId;
  protected int indexReadLimit;
}
