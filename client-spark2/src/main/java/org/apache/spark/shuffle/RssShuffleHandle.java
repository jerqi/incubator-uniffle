package org.apache.spark.shuffle;

import com.tencent.rss.common.ShuffleServerHandler;
import org.apache.spark.ShuffleDependency;

public class RssShuffleHandle<K, V, C> extends ShuffleHandle {

    private String appId;
    private int numMaps;
    private ShuffleDependency<K, V, C> dependency;
    private ShuffleServerHandler shuffleServerHandler;

    public RssShuffleHandle(int shuffleId, String appId, int numMaps,
            ShuffleDependency<K, V, C> dependency, ShuffleServerHandler shuffleServerHandler) {
        super(shuffleId);
        this.appId = appId;
        this.numMaps = numMaps;
        this.dependency = dependency;
        this.shuffleServerHandler = shuffleServerHandler;
    }

    public String getAppId() {
        return appId;
    }

    public int getNumMaps() {
        return numMaps;
    }

    public ShuffleDependency<K, V, C> getDependency() {
        return dependency;
    }

    public ShuffleServerHandler getShuffleServerHandler() {
        return shuffleServerHandler;
    }

    public int getShuffleId() {
        return shuffleId();
    }
}
