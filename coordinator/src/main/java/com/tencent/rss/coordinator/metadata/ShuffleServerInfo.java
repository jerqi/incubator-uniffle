package com.tencent.rss.coordinator.metadata;

import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class to maintain metadata for {@code ApplicationInfo} in a shuffle server
 */
public class ShuffleServerInfo {
    private final ShuffleServerId shuffleServerId;
    // Note: we use Map here instead of Set to facilitate fast indexing
    private final Map<String, ApplicationInfo> applicationInfos;

    private ShuffleServerInfo(ShuffleServerId shuffleServerId,
            Map<String, ApplicationInfo> applicationInfos) {
        this.shuffleServerId = shuffleServerId;
        this.applicationInfos = applicationInfos;
    }

    public static ShuffleServerInfo build(ShuffleServerId shuffleServerId) {
        return new ShuffleServerInfo(shuffleServerId, new HashMap<>());
    }

    public static ShuffleServerInfo build(ShuffleServerId shuffleServerId,
            Map<String, ApplicationInfo> applicationInfos) {
        return new ShuffleServerInfo(shuffleServerId, applicationInfos);
    }

    public void addApplicationInfo(ApplicationInfo applicationInfo) {
        final String appId = applicationInfo.getAppId();
        if (applicationInfos.containsKey(appId)) {
            for (ShuffleInfo shuffleInfo: applicationInfo.getShuffleInfos().values()) {
                applicationInfos.get(appId).addShuffleInfo(shuffleInfo);
            }
        } else {
            applicationInfos.put(applicationInfo.getAppId(), applicationInfo);
        }
    }

    public ShuffleServerId getShuffleServerId() {
        return shuffleServerId;
    }

    public Map<String, ApplicationInfo> getApplicationInfos() {
        return applicationInfos;
    }

    public int countApplications() {
        return applicationInfos.size();
    }

    public int countShuffles() {
        int total = 0;
        for (ApplicationInfo applicationInfo: applicationInfos.values()) {
            total = total + applicationInfo.countShuffles();
        }
        return total;
    }

    public int countPartitions() {
        int total = 0;
        for (ApplicationInfo applicationInfo: applicationInfos.values()) {
            total = total + applicationInfo.countPartitions();
        }
        return total;
    }

    public int countPartitionRanges() {
        int total = 0;
        for (ApplicationInfo applicationInfo: applicationInfos.values()) {
            total = total + applicationInfo.countPartitionRanges();
        }
        return total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShuffleServerInfo that = (ShuffleServerInfo) o;
        return Objects.equals(shuffleServerId, that.shuffleServerId)
                && Objects.equals(applicationInfos, that.applicationInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shuffleServerId, applicationInfos);
    }

    @Override
    public String toString() {
        return "ShuffleServerInfo{"
                + "shuffleServerId=" + shuffleServerId
                + ", applicationInfos=" + applicationInfos + '}';
    }
}
