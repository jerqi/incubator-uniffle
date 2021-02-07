package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.handler.api.ShuffleReadHandler;

public abstract class AbstractFileShuffleReadHandler implements ShuffleReadHandler {

  protected String appId;
  protected int shuffleId;
  protected int partitionId;
  protected int indexReadLimit;
}
