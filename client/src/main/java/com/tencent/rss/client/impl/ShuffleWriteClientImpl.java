package com.tencent.rss.client.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.factory.ShuffleServerClientFactory;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssGetShuffleAssignmentsRequest;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.ClientResponse;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssAppHeartBeatResponse;
import com.tencent.rss.client.response.RssFinishShuffleResponse;
import com.tencent.rss.client.response.RssGetShuffleAssignmentsResponse;
import com.tencent.rss.client.response.RssGetShuffleResultResponse;
import com.tencent.rss.client.response.RssRegisterShuffleResponse;
import com.tencent.rss.client.response.RssReportShuffleResultResponse;
import com.tencent.rss.client.response.RssSendCommitResponse;
import com.tencent.rss.client.response.RssSendShuffleDataResponse;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleWriteClientImpl implements ShuffleWriteClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleWriteClientImpl.class);

  private String clientType;
  private int retryMax;
  private long retryInterval;
  private CoordinatorClient coordinatorClient;
  private CoordinatorClientFactory coordinatorClientFactory;

  public ShuffleWriteClientImpl(String clientType, int retryMax, long retryInterval) {
    this.clientType = clientType;
    this.retryMax = retryMax;
    this.retryInterval = retryInterval;
    coordinatorClientFactory = new CoordinatorClientFactory(clientType);
  }

  private void sendShuffleDataAsync(
      String appId,
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks,
      Map<ShuffleServerInfo, List<Long>> serverToBlockIds,
      Set<Long> successBlockIds,
      Set<Long> tempFailedBlockIds) {
    if (serverToBlocks != null) {
      serverToBlocks.entrySet().parallelStream().forEach(entry -> {
        ShuffleServerInfo ssi = entry.getKey();
        try {
          RssSendShuffleDataRequest request = new RssSendShuffleDataRequest(
              appId, retryMax, retryInterval, entry.getValue());
          long s = System.currentTimeMillis();
          RssSendShuffleDataResponse response = getShuffleServerClient(ssi).sendShuffleData(request);
          LOG.info("ShuffleWriteClientImpl sendShuffleData cost:" + (System.currentTimeMillis() - s));

          if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
            successBlockIds.addAll(serverToBlockIds.get(ssi));
            LOG.debug("Send: " + serverToBlockIds.get(ssi)
                + " to [" + ssi.getId() + "] successfully");
          } else {
            tempFailedBlockIds.addAll(serverToBlockIds.get(ssi));
            LOG.error("Send: " + serverToBlockIds.get(ssi) + " to [" + ssi.getId() + "] temp failed.");
          }
        } catch (Exception e) {
          tempFailedBlockIds.addAll(serverToBlockIds.get(ssi));
          LOG.error("Send: " + serverToBlockIds.get(ssi) + " to [" + ssi.getId() + "] temp failed.", e);
        }
      });
    }
  }

  @Override
  public SendShuffleDataResult sendShuffleData(String appId, List<ShuffleBlockInfo> shuffleBlockInfoList) {

    // shuffleServer -> shuffleId -> partitionId -> blocks
    Map<ShuffleServerInfo, Map<Integer,
        Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks = Maps.newHashMap();
    Map<ShuffleServerInfo, List<Long>> serverToBlockIds = Maps.newHashMap();
    // send shuffle block to shuffle server
    // for all ShuffleBlockInfo, create the data structure as shuffleServer -> shuffleId -> partitionId -> blocks
    // it will be helpful to send rpc request to shuffleServer
    for (ShuffleBlockInfo sbi : shuffleBlockInfoList) {
      int partitionId = sbi.getPartitionId();
      int shuffleId = sbi.getShuffleId();
      for (ShuffleServerInfo ssi : sbi.getShuffleServerInfos()) {
        if (!serverToBlockIds.containsKey(ssi)) {
          serverToBlockIds.put(ssi, Lists.newArrayList());
        }
        serverToBlockIds.get(ssi).add(sbi.getBlockId());

        if (!serverToBlocks.containsKey(ssi)) {
          serverToBlocks.put(ssi, Maps.newHashMap());
        }
        Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = serverToBlocks.get(ssi);
        if (!shuffleIdToBlocks.containsKey(shuffleId)) {
          shuffleIdToBlocks.put(shuffleId, Maps.newHashMap());
        }

        Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = shuffleIdToBlocks.get(shuffleId);
        if (!partitionToBlocks.containsKey(partitionId)) {
          partitionToBlocks.put(partitionId, Lists.newArrayList());
        }
        partitionToBlocks.get(partitionId).add(sbi);
      }
    }

    Set<Long> tempFailedBlockIds = Sets.newConcurrentHashSet();
    Set<Long> successBlockIds = Sets.newConcurrentHashSet();
    sendShuffleDataAsync(appId, serverToBlocks, serverToBlockIds, successBlockIds, tempFailedBlockIds);
    if (!successBlockIds.containsAll(tempFailedBlockIds)) {
      tempFailedBlockIds.removeAll(successBlockIds);
      LOG.error("Send: " + tempFailedBlockIds + " failed.");
    }

    return new SendShuffleDataResult(successBlockIds, tempFailedBlockIds);
  }

  @Override
  public void sendCommit(Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps) {
    shuffleServerInfoSet.parallelStream().forEach(ssi -> {
      LOG.info("SendCommit for appId[" + appId + "], shuffleId[" + shuffleId
          + "] to ShuffleServer[" + ssi.getId() + "]");
      RssSendCommitRequest request = new RssSendCommitRequest(appId, shuffleId);
      RssSendCommitResponse response = getShuffleServerClient(ssi).sendCommit(request);

      String msg = "Can't commit shuffle data to " + ssi
          + " for [appId=" + request.getAppId() + ", shuffleId=" + shuffleId + "]";
      throwExceptionIfNecessary(response, msg);

      LOG.info("Got committed maps[" + response.getCommitCount() + "], map number of stage is " + numMaps);
      if (response.getCommitCount() >= numMaps) {
        RssFinishShuffleResponse rfsResponse =
            getShuffleServerClient(ssi).finishShuffle(new RssFinishShuffleRequest(appId, shuffleId));
        msg = "Can't finish shuffle commit to " + ssi
            + " for [appId=" + request.getAppId() + ", shuffleId=" + shuffleId + "]";
        throwExceptionIfNecessary(rfsResponse, msg);
      }
    });
  }

  @Override
  public void registerShuffle(
      ShuffleServerInfo shuffleServerInfo, String appId, int shuffleId, int start, int end) {
    RssRegisterShuffleRequest request = new RssRegisterShuffleRequest(appId, shuffleId, start, end);
    RssRegisterShuffleResponse response = getShuffleServerClient(shuffleServerInfo).registerShuffle(request);

    String msg = "Error happend when registerShuffle with appId[" + appId + "], shuffleId[" + shuffleId
        + "], start[" + start + "], end[" + end + "] to " + shuffleServerInfo;
    throwExceptionIfNecessary(response, msg);
  }

  @Override
  public void registerCoordinatorClient(String host, int port) {
    if (coordinatorClient == null) {
      coordinatorClient = coordinatorClientFactory.createCoordinatorClient(host, port);
    }
  }

  @Override
  public ShuffleAssignmentsInfo getShuffleAssignments(
      String appId, int shuffleId, int partitionNum, int partitionNumPerRange, int dataReplica) {
    RssGetShuffleAssignmentsRequest request = new RssGetShuffleAssignmentsRequest(
        appId, shuffleId, partitionNum, partitionNumPerRange, dataReplica);
    RssGetShuffleAssignmentsResponse response = coordinatorClient.getShuffleAssignments(request);
    String msg = "Error happend when getShuffleAssignments with appId[" + appId + "], shuffleId[" + shuffleId
        + "], numMaps[" + partitionNum + "], partitionNumPerRange[" + partitionNumPerRange + "] to coordinator";
    throwExceptionIfNecessary(response, msg);
    return new ShuffleAssignmentsInfo(response.getPartitionToServers(),
        response.getRegisterInfoList(), response.getShuffleServersForResult());
  }

  @Override
  public void reportShuffleResult(Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId, int shuffleId, Map<Integer, List<Long>> partitionToBlockIds) {
    RssReportShuffleResultRequest request = new RssReportShuffleResultRequest(appId, shuffleId, partitionToBlockIds);
    boolean isSuccessful = false;
    for (ShuffleServerInfo ssi : shuffleServerInfoSet) {
      try {
        RssReportShuffleResultResponse response = getShuffleServerClient(ssi).reportShuffleResult(request);
        if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
          isSuccessful = true;
        }
      } catch (Exception e) {
        LOG.warn("Report shuffle result is failed to " + ssi
            + " for appId[" + appId + "], shuffleId[" + shuffleId + "]");
      }
    }
    if (!isSuccessful) {
      throw new RuntimeException("Report shuffle result is failed for appId["
          + appId + "], shuffleId[" + shuffleId + "]");
    }
  }

  @Override
  public List<Long> getShuffleResult(String clientType, Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId, int shuffleId, int partitionId) {
    RssGetShuffleResultRequest request = new RssGetShuffleResultRequest(appId, shuffleId, partitionId);
    boolean isSuccessful = false;
    List<Long> blockIds = Lists.newArrayList();
    for (ShuffleServerInfo ssi : shuffleServerInfoSet) {
      try {
        RssGetShuffleResultResponse response = ShuffleServerClientFactory
            .getInstance().getShuffleServerClient(clientType, ssi).getShuffleResult(request);
        if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
          blockIds = response.getBlockIds();
          isSuccessful = true;
          break;
        }
      } catch (Exception e) {
        LOG.warn("Get shuffle result is failed from " + ssi
            + " for appId[" + appId + "], shuffleId[" + shuffleId + "]");
      }
    }
    if (!isSuccessful) {
      throw new RuntimeException("Get shuffle result is failed for appId["
          + appId + "], shuffleId[" + shuffleId + "]");
    }
    return blockIds;
  }

  @Override
  public void sendAppHeartbeat(String appId) {
    RssAppHeartBeatRequest request = new RssAppHeartBeatRequest(appId);
    RssAppHeartBeatResponse response = coordinatorClient.sendAppHeartBeat(request);
    if (response.getStatusCode() != ResponseStatusCode.SUCCESS) {
      LOG.warn("Send heartbeat failed for application[" + appId + "]");
    }
  }

  @Override
  public void close() {
    if (coordinatorClient != null) {
      coordinatorClient.close();
    }
  }

  private void throwExceptionIfNecessary(ClientResponse response, String errorMsg) {
    if (response.getStatusCode() != ResponseStatusCode.SUCCESS) {
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
  }

  @VisibleForTesting
  protected ShuffleServerClient getShuffleServerClient(ShuffleServerInfo shuffleServerInfo) {
    return ShuffleServerClientFactory.getInstance().getShuffleServerClient(clientType, shuffleServerInfo);
  }
}
