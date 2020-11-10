package com.tencent.rss.common;

public class ShuffleServerHandler {

    private ShuffleServerInfo[] shuffleServerInfos;

    private int[] partitions;

    public ShuffleServerHandler() {
    }

    public ShuffleServerHandler(ShuffleServerInfo[] shuffleServerInfos, int[] partitions) {
        this.shuffleServerInfos = shuffleServerInfos;
        this.partitions = partitions;
    }

    public ShuffleServerInfo[] getShuffleServerInfos() {
        return shuffleServerInfos;
    }

    public void setShuffleServerInfos(ShuffleServerInfo[] shuffleServerInfos) {
        this.shuffleServerInfos = shuffleServerInfos;
    }

    public int[] getPartitions() {
        return partitions;
    }

    public void setPartitions(int[] partitions) {
        this.partitions = partitions;
    }
}
