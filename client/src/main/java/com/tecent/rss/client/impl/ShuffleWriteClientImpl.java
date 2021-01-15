package com.tecent.rss.client.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tecent.rss.client.CoordinatorClient;
import com.tecent.rss.client.ShuffleServerClient;
import com.tecent.rss.client.ShuffleWriteClient;
import com.tecent.rss.client.factory.CoordinatorClientFactory;
import com.tecent.rss.client.factory.ShuffleServerClientFactory;
import com.tecent.rss.client.request.GetShuffleAssignmentsRequest;
import com.tecent.rss.client.request.RegisterShuffleRequest;
import com.tecent.rss.client.request.SendCommitRequest;
import com.tecent.rss.client.request.SendShuffleDataRequest;
import com.tecent.rss.client.response.ClientResponse;
import com.tecent.rss.client.response.GetShuffleAssignmentsResponse;
import com.tecent.rss.client.response.RegisterShuffleResponse;
import com.tecent.rss.client.response.ResponseStatusCode;
import com.tecent.rss.client.response.SendCommitResponse;
import com.tecent.rss.client.response.SendShuffleDataResponse;
import com.tecent.rss.client.response.SendShuffleDataResult;
import com.tecent.rss.client.response.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleWriteClientImpl implements ShuffleWriteClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleWriteClientImpl.class);
  private static final Logger LOG_RSS_INFO = LoggerFactory.getLogger(Constants.LOG4J_RSS_SHUFFLE_PREFIX);

  private String clientType;
  private CoordinatorClient coordinatorClient;
  private Map<ShuffleServerInfo, ShuffleServerClient> shuffleServerClients;
  private CoordinatorClientFactory coordinatorClientFactory;
  private ShuffleServerClientFactory shuffleServerClientFactory;

  public ShuffleWriteClientImpl(String clientType) {
    this.clientType = clientType;
    coordinatorClientFactory = new CoordinatorClientFactory(clientType);
    shuffleServerClientFactory = new ShuffleServerClientFactory(clientType);
    shuffleServerClients = Maps.newHashMap();
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

    List<Long> tempFailedBlockIds = Lists.newArrayList();
    List<Long> successBlockIds = Lists.newArrayList();
    for (Map.Entry<ShuffleServerInfo, Map<Integer,
        Map<Integer, List<ShuffleBlockInfo>>>> entry : serverToBlocks.entrySet()) {
      ShuffleServerInfo ssi = entry.getKey();
      try {
        SendShuffleDataRequest rpcRequest = new SendShuffleDataRequest(appId, entry.getValue());
        SendShuffleDataResponse rpcResponse = getShuffleServerClient(ssi).sendShuffleData(rpcRequest);
        if (rpcResponse.getStatusCode() == ResponseStatusCode.SUCCESS) {
          successBlockIds.addAll(serverToBlockIds.get(ssi));
          LOG.info("Send: " + serverToBlockIds.get(ssi)
              + " to [" + ssi.getId() + "] successfully");
        } else {
          tempFailedBlockIds.addAll(serverToBlockIds.get(ssi));
          LOG.error("Send: " + serverToBlockIds.get(ssi) + " to [" + ssi.getId() + "] temp failed.");
        }
      } catch (Exception e) {
        tempFailedBlockIds.addAll(serverToBlockIds.get(ssi));
        LOG.error("Send: " + serverToBlockIds.get(ssi) + " to [" + ssi.getId() + "] temp failed.", e);
      }
    }
    if (!successBlockIds.containsAll(tempFailedBlockIds)) {
      tempFailedBlockIds.removeAll(successBlockIds);
      LOG.error("Send: " + tempFailedBlockIds + " failed.");
    }

    return new SendShuffleDataResult(successBlockIds, tempFailedBlockIds);
  }

  @Override
  public void sendCommit(Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId) {
    shuffleServerInfoSet.parallelStream().forEach(ssi -> {
      LOG.info("SendCommit for appId[" + appId + "], shuffleId[" + shuffleId
          + "] to ShuffleServer[" + ssi.getId() + "]");
      SendCommitRequest request = new SendCommitRequest(appId, shuffleId);
      SendCommitResponse response = getShuffleServerClient(ssi).sendCommit(request);
      String msg = "Can't commit shuffle data to " + ssi
          + " for [appId=" + request.getAppId() + ", shuffleId=" + shuffleId + "]";
      throwExceptionIfNecessary(response, msg);
    });
  }

  @Override
  public void registerShuffle(
      ShuffleServerInfo shuffleServerInfo, String appId, int shuffleId, int start, int end) {
    RegisterShuffleRequest request = new RegisterShuffleRequest(appId, shuffleId, start, end);
    RegisterShuffleResponse response = getShuffleServerClient(shuffleServerInfo).registerShuffle(request);
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
      String appId, int shuffleId, int partitionNum, int partitionsPerServer) {
    GetShuffleAssignmentsRequest request = new GetShuffleAssignmentsRequest(
        appId, shuffleId, partitionNum, partitionsPerServer);
    GetShuffleAssignmentsResponse response = coordinatorClient.getShuffleAssignments(request);
    String msg = "Error happend when getShuffleAssignments with appId[" + appId + "], shuffleId[" + shuffleId
        + "], numMaps[" + partitionNum + "], partitionsPerServer[" + partitionsPerServer + "] to coordinator";
    throwExceptionIfNecessary(response, msg);
    return new ShuffleAssignmentsInfo(response.getPartitionToServers(), response.getRegisterInfoList());
  }

  @Override
  public void close() {
    if (coordinatorClient != null) {
      coordinatorClient.close();
    }
    if (shuffleServerClients != null) {
      for (ShuffleServerClient ssc : shuffleServerClients.values()) {
        ssc.close();
      }
    }
  }

  private void throwExceptionIfNecessary(ClientResponse response, String errorMsg) {
    if (response.getStatusCode() != ResponseStatusCode.SUCCESS) {
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
  }

  private synchronized ShuffleServerClient getShuffleServerClient(ShuffleServerInfo shuffleServerInfo) {
    if (shuffleServerClients.get(shuffleServerInfo) == null) {
      shuffleServerClients.put(
          shuffleServerInfo, shuffleServerClientFactory.createShuffleServerClient(
              shuffleServerInfo.getHost(), shuffleServerInfo.getPort()));
    }
    return shuffleServerClients.get(shuffleServerInfo);
  }
}
