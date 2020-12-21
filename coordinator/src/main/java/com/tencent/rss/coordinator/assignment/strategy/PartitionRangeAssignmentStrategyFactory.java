package com.tencent.rss.coordinator.assignment.strategy;

import com.tencent.rss.coordinator.CoordinatorConf;

public class PartitionRangeAssignmentStrategyFactory {
    public static PartitionRangeAssignmentStrategy build(CoordinatorConf conf) {
        return new BasicPartitionRangeAssignmentStrategy();
    }
}
