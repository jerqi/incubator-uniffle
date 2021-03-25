package org.apache.spark.shuffle;

import com.google.common.collect.Sets;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.ShuffleDependency;

public class RssShuffleHandle<K, V, C> extends ShuffleHandle {

  private String appId;
  private int numMaps;
  private ShuffleDependency<K, V, C> dependency;
  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  // shuffle servers which store the information of partition -> blockIds
  private Set<ShuffleServerInfo> shuffleServersForResult;
  // shuffle servers which is for store shuffle data
  private Set<ShuffleServerInfo> shuffleServersForData;

  public RssShuffleHandle(int shuffleId, String appId, int numMaps,
      ShuffleDependency<K, V, C> dependency,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      Set<ShuffleServerInfo> shuffleServersForResult) {
    super(shuffleId);
    this.appId = appId;
    this.numMaps = numMaps;
    this.dependency = dependency;
    this.partitionToServers = partitionToServers;
    this.shuffleServersForResult = shuffleServersForResult;
    shuffleServersForData = Sets.newHashSet();
    for (List<ShuffleServerInfo> ssis : partitionToServers.values()) {
      shuffleServersForData.addAll(ssis);
    }
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

  public Set<ShuffleServerInfo> getShuffleServersForResult() {
    return shuffleServersForResult;
  }

  public Set<ShuffleServerInfo> getShuffleServersForData() {
    return shuffleServersForData;
  }
}
