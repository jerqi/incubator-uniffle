package com.tencent.rss.common.config;

import java.util.List;
import java.util.Map;


public class RssBaseConf extends RssConf {

  public static final ConfigOption<String> RSS_COORDINATOR_IP = ConfigOptions
      .key("rss.coordinator.ip")
      .stringType()
      .noDefaultValue()
      .withDescription("Coordinator ip");

  public static final ConfigOption<Integer> RSS_COORDINATOR_PORT = ConfigOptions
      .key("rss.coordinator.port")
      .intType()
      .noDefaultValue()
      .withDescription("Coordinator port");

  public static final ConfigOption<String> RSS_COORDINATOR_QUORUM = ConfigOptions
      .key("rss.coordinator.quorum")
      .stringType()
      .noDefaultValue()
      .withDescription("Coordinator quorum");

  public static final ConfigOption<String> RPC_SERVER_TYPE = ConfigOptions
      .key("rss.rpc.server.type")
      .stringType()
      .defaultValue("GRPC")
      .withDescription("Shuffle server type, default is grpc");

  public static final ConfigOption<Integer> RPC_SERVER_PORT = ConfigOptions
      .key("rss.rpc.server.port")
      .intType()
      .noDefaultValue()
      .withDescription("Shuffle server service port");
  public static final ConfigOption<Integer> JETTY_HTTP_PORT = ConfigOptions
      .key("rss.jetty.http.port")
      .intType()
      .noDefaultValue()
      .withDescription("jetty http port");

  public static final ConfigOption<Integer> JETTY_MAX_THREAD = ConfigOptions
      .key("rss.jetty.max.thread")
      .intType()
      .defaultValue(16)
      .withDescription("jetty max thread");

  public static final ConfigOption<Integer> JETTY_MIN_THREAD = ConfigOptions
      .key("rss.jetty.min.thread")
      .intType()
      .defaultValue(8)
      .withDescription("jetty min thread");

  public static final ConfigOption<Integer> JETTY_CORE_POOL_SIZE = ConfigOptions
      .key("rss.jetty.corePool.size")
      .intType()
      .defaultValue(256)
      .withDescription("jetty corePool size");

  public static final ConfigOption<Integer> JETTY_MAX_POOL_SIZE = ConfigOptions
      .key("rss.jetty.maxPool.size")
      .intType()
      .defaultValue(256)
      .withDescription("jetty max pool size");

  public static final ConfigOption<Integer> JETTY_QUEUE_SIZE = ConfigOptions
      .key("rss.jetty.queue.size")
      .intType()
      .defaultValue(128)
      .withDescription("jetty queue size");

  public static final ConfigOption<Integer> JETTY_HEADER_BUFFER_SIZE = ConfigOptions
      .key("rss.jetty.header.buffer.size")
      .intType()
      .defaultValue(10 * 1024 * 1024)
      .withDescription("jetty ssl port");

  public static final ConfigOption<Boolean> JETTY_SSL_ENABLE = ConfigOptions
      .key("rss.jetty.ssl.enable")
      .booleanType()
      .defaultValue(false)
      .withDescription("jetty ssl enable");

  public static final ConfigOption<Integer> JETTY_HTTPS_PORT = ConfigOptions
      .key("rss.jetty.https.port")
      .intType()
      .noDefaultValue()
      .withDescription("jetty https port");

  public static final ConfigOption<String> JETTY_SSL_KEYSTORE_PATH = ConfigOptions
      .key("rss.jetty.ssl.keystore.path")
      .stringType()
      .noDefaultValue()
      .withDescription("jetty ssl keystore path");

  public static final ConfigOption<String> JETTY_SSL_KEYMANAGER_PASSWORD = ConfigOptions
      .key("rss.jetty.ssl.keymanager.password")
      .stringType()
      .noDefaultValue()
      .withDescription("jetty ssl keymanager password");

  public static final ConfigOption<String> JETTY_SSL_KEYSTORE_PASSWORD = ConfigOptions
      .key("rss.jetty.ssl.keystore.password")
      .stringType()
      .noDefaultValue()
      .withDescription("jetty ssl keystore password");

  public static final ConfigOption<String> JETTY_SSL_TRUSTSTORE_PASSWORD = ConfigOptions
      .key("rss.jetty.ssl.truststore.password")
      .stringType()
      .noDefaultValue()
      .withDescription("jetty ssl truststore password");

  public static final ConfigOption<Long> JETTY_STOP_TIMEOUT = ConfigOptions
      .key("rss.jetty.stop.timeout")
      .longType()
      .defaultValue(30 * 1000L)
      .withDescription("jetty stop timeout (ms) ");

  public static final ConfigOption<Long> JETTY_HTTP_IDLE_TIMEOUT = ConfigOptions
      .key("rss.jetty.http.idle.timeout")
      .longType()
      .defaultValue(30 * 1000L)
      .withDescription("jetty http idle timeout (ms) ");

  public static final ConfigOption<Integer> RPC_MESSAGE_MAX_SIZE = ConfigOptions
      .key("rss.rpc.message.max.size")
      .intType()
      .defaultValue(Integer.MAX_VALUE)
      .withDescription("Max size of rpc message (byte)");

  public static final ConfigOption<String> RSS_CLIENT_TYPE = ConfigOptions
      .key("rss.rpc.client.type")
      .stringType()
      .defaultValue("GRPC")
      .withDescription("client type for rss");

  public static final ConfigOption<String> RSS_STORAGE_TYPE = ConfigOptions
      .key("rss.storage.type")
      .stringType()
      .noDefaultValue()
      .withDescription("Data storage for remote shuffle service");

  public static final ConfigOption<Integer> RSS_STORAGE_DATA_REPLICA = ConfigOptions
      .key("rss.storage.data.replica")
      .intType()
      .defaultValue(1)
      .withDescription("Data replica in storage");

  public static final ConfigOption<String> RSS_STORAGE_BASE_PATH = ConfigOptions
      .key("rss.storage.basePath")
      .stringType()
      .noDefaultValue()
      .withDescription("Common storage path for remote shuffle data");

  public static final ConfigOption<Integer> RSS_STORAGE_INDEX_READ_LIMIT = ConfigOptions
      .key("rss.storage.index.read.limit")
      .intType()
      .defaultValue(1000)
      .withDescription("Read index entity number");

  public static final ConfigOption<Integer> RPC_EXECUTOR_SIZE = ConfigOptions
      .key("rss.rpc.executor.size")
      .intType()
      .defaultValue(1000)
      .withDescription("Thread number for grpc to process request");


  public boolean loadCommonConf(Map<String, String> properties) {
    if (properties == null) {
      return false;
    }

    List<ConfigOption> configOptions = ConfigUtils.getAllConfigOptions(RssBaseConf.class);
    properties.forEach((k, v) -> {
      configOptions.forEach(config -> {
        if (config.key().equalsIgnoreCase(k)) {
          set(config, ConfigUtils.convertValue(v, config.getClazz()));
        }
      });
    });

    return true;
  }

}
