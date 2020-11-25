package org.apache.spark.shuffle;

import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;
import org.apache.spark.ShuffleDependency;

public class RssShuffleHandle<K, V, C> extends ShuffleHandle {

    private String appId;
    private int numMaps;
    private ShuffleDependency<K, V, C> dependency;
    private Map<Integer, List<ShuffleServerInfo>> partitionToServers;

    public RssShuffleHandle(int shuffleId, String appId, int numMaps,
            ShuffleDependency<K, V, C> dependency, Map<Integer, List<ShuffleServerInfo>> partitionToServers) {
        super(shuffleId);
        this.appId = appId;
        this.numMaps = numMaps;
        this.dependency = dependency;
        this.partitionToServers = partitionToServers;
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

    public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
        return partitionToServers;
    }

    public int getShuffleId() {
        return shuffleId();
    }
}
