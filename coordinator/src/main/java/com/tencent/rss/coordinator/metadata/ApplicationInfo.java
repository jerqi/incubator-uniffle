package com.tencent.rss.coordinator.metadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class to maintain metadata for {@code ShuffleInfo} in application
 */
public class ApplicationInfo {
    private final String appId;
    // Note: we use Map here instead of Set to facilitate fast indexing
    private final Map<Integer, ShuffleInfo> shuffleInfos;

    private ApplicationInfo(String appId, Map<Integer, ShuffleInfo> shuffleInfos) {
        this.appId = appId;
        this.shuffleInfos = shuffleInfos;
    }

    public static ApplicationInfo build(String appId) {
        return new ApplicationInfo(appId, new HashMap<>());
    }

    public static ApplicationInfo build(String appId, Map<Integer, ShuffleInfo> shuffleInfos) {
        return new ApplicationInfo(appId, shuffleInfos);
    }

    public void addShuffleInfo(ShuffleInfo shuffleInfo) {
        final int shuffleId = shuffleInfo.getShuffleId();
        if (shuffleInfos.containsKey(shuffleId)) {
            shuffleInfos.get(shuffleId).addAllPartitionRanges(shuffleInfo.getPartitionRangeSet());
        } else {
            shuffleInfos.put(shuffleInfo.getShuffleId(), shuffleInfo);
        }
    }

    public String getAppId() {
        return appId;
    }

    public int countShuffles() {
        return shuffleInfos.size();
    }

    public int countPartitions() {
        int total = 0;
        for (ShuffleInfo shuffleInfo: shuffleInfos.values()) {
            total = total + shuffleInfo.countPartitions();
        }
        return total;
    }

    public int countPartitionRanges() {
        int total = 0;
        for (ShuffleInfo shuffleInfo: shuffleInfos.values()) {
            total = total + shuffleInfo.countPartitionRanges();
        }
        return total;
    }

    public Map<Integer, ShuffleInfo> getShuffleInfos() {
        return shuffleInfos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ApplicationInfo that = (ApplicationInfo) o;
        return Objects.equals(appId, that.appId)
                && Objects.equals(shuffleInfos, that.shuffleInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(appId, shuffleInfos);
    }

    @Override
    public String toString() {
        return "ApplicationInfo{" + "appId='" + appId + '\''
                + ", shuffleInfos=" + shuffleInfos + '}';
    }
}
