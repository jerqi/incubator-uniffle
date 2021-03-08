package com.tencent.rss.client.api;

import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.RssFinishShuffleResponse;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.client.response.RssGetShuffleResultResponse;
import com.tencent.rss.client.response.RssRegisterShuffleResponse;
import com.tencent.rss.client.response.RssReportShuffleResultResponse;
import com.tencent.rss.client.response.RssSendCommitResponse;
import com.tencent.rss.client.response.RssSendShuffleDataResponse;

public interface ShuffleServerClient {

  RssRegisterShuffleResponse registerShuffle(RssRegisterShuffleRequest request);

  RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request);

  RssSendCommitResponse sendCommit(RssSendCommitRequest request);

  RssFinishShuffleResponse finishShuffle(RssFinishShuffleRequest request);

  RssReportShuffleResultResponse reportShuffleResult(RssReportShuffleResultRequest request);

  RssGetShuffleResultResponse getShuffleResult(RssGetShuffleResultRequest request);

  RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request);

  void close();

  String getClientInfo();
}
