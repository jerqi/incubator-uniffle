package org.apache.spark.shuffle;

import com.tecent.rss.client.ShuffleServerHandler;
import org.apache.spark.ShuffleDependency;

public class RssShuffleHandle<K, V, C> extends ShuffleHandle {

    private String appId;
    private int numMaps;
    private ShuffleDependency<K, V, C> dependency;
    private ShuffleServerHandler[] rssServers;

    public RssShuffleHandle(int shuffleId, String appId, int numMaps,
            ShuffleDependency<K, V, C> dependency, ShuffleServerHandler[] rssServers) {
        super(shuffleId);
        this.appId = appId;
        this.numMaps = numMaps;
        this.dependency = dependency;
        this.rssServers = rssServers;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public int getNumMaps() {
        return numMaps;
    }

    public void setNumMaps(int numMaps) {
        this.numMaps = numMaps;
    }

    public ShuffleDependency<K, V, C> getDependency() {
        return dependency;
    }

    public void setDependency(ShuffleDependency<K, V, C> dependency) {
        this.dependency = dependency;
    }

    public ShuffleServerHandler[] getRssServers() {
        return rssServers;
    }

    public void setRssServers(ShuffleServerHandler[] rssServers) {
        this.rssServers = rssServers;
    }

    public int getShuffleId() {
        return shuffleId();
    }
}
