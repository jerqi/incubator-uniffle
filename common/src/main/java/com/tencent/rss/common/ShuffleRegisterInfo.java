/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.rss.common;

import java.util.List;

public class ShuffleRegisterInfo {

  private ShuffleServerInfo shuffleServerInfo;
  private List<PartitionRange> partitionRanges;

  public ShuffleRegisterInfo(ShuffleServerInfo shuffleServerInfo, List<PartitionRange> partitionRanges) {
    this.shuffleServerInfo = shuffleServerInfo;
    this.partitionRanges = partitionRanges;
  }

  public ShuffleServerInfo getShuffleServerInfo() {
    return shuffleServerInfo;
  }

  public List<PartitionRange> getPartitionRanges() {
    return partitionRanges;
  }

  @Override
  public int hashCode() {
    return shuffleServerInfo.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ShuffleRegisterInfo) {
      return shuffleServerInfo.equals(((ShuffleRegisterInfo) obj).getShuffleServerInfo())
          && partitionRanges.equals(((ShuffleRegisterInfo) obj).getPartitionRanges());
    }
    return false;
  }

  @Override
  public String toString() {
    return "ShuffleRegisterInfo: shuffleServerInfo[" + shuffleServerInfo.getId() + "], " + partitionRanges;
  }
}