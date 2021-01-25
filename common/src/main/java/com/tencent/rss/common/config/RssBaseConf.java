package com.tencent.rss.common.config;

import java.util.Map;

public class RssBaseConf extends RssConf {

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

  public boolean loadCommonConf(Map<String, String> properties) {
    if (properties == null) {
      return false;
    }

    properties.forEach((k, v) -> {
      if (RPC_SERVER_TYPE.key().equalsIgnoreCase(k)) {
        set(RPC_SERVER_TYPE, v);
      }

      if (RPC_SERVER_PORT.key().equalsIgnoreCase(k)) {
        set(RPC_SERVER_PORT, Integer.valueOf(v));
      }

      if (JETTY_HTTP_PORT.key().equalsIgnoreCase(k)) {
        set(JETTY_HTTP_PORT, Integer.valueOf(v));
      }

      if (JETTY_MAX_THREAD.key().equalsIgnoreCase(k)) {
        set(JETTY_MAX_THREAD, Integer.valueOf(v));
      }

      if (JETTY_MIN_THREAD.key().equalsIgnoreCase(k)) {
        set(JETTY_MIN_THREAD, Integer.valueOf(v));
      }

      if (JETTY_CORE_POOL_SIZE.key().equalsIgnoreCase(k)) {
        set(JETTY_CORE_POOL_SIZE, Integer.valueOf(v));
      }

      if (JETTY_MAX_POOL_SIZE.key().equalsIgnoreCase(k)) {
        set(JETTY_MAX_POOL_SIZE, Integer.valueOf(v));
      }

      if (JETTY_QUEUE_SIZE.key().equalsIgnoreCase(k)) {
        set(JETTY_QUEUE_SIZE, Integer.valueOf(v));
      }

      if (JETTY_HEADER_BUFFER_SIZE.key().equalsIgnoreCase(k)) {
        set(JETTY_HEADER_BUFFER_SIZE, Integer.valueOf(v));
      }

      if (JETTY_HTTPS_PORT.key().equalsIgnoreCase(k)) {
        set(JETTY_HTTPS_PORT, Integer.valueOf(v));
      }

      if (JETTY_SSL_KEYSTORE_PATH.key().equalsIgnoreCase(k)) {
        set(JETTY_SSL_KEYSTORE_PATH, v);
      }

      if (JETTY_SSL_KEYSTORE_PASSWORD.key().equalsIgnoreCase(k)) {
        set(JETTY_SSL_KEYSTORE_PASSWORD, v);
      }

      if (JETTY_SSL_TRUSTSTORE_PASSWORD.key().equalsIgnoreCase(k)) {
        set(JETTY_SSL_TRUSTSTORE_PASSWORD, v);
      }

      if (JETTY_SSL_KEYMANAGER_PASSWORD.key().equalsIgnoreCase(k)) {
        set(JETTY_SSL_KEYMANAGER_PASSWORD, v);
      }

      if (JETTY_STOP_TIMEOUT.key().equalsIgnoreCase(k)) {
        set(JETTY_STOP_TIMEOUT, Long.valueOf(v));
      }

      if (JETTY_HTTP_IDLE_TIMEOUT.key().equalsIgnoreCase(k)) {
        set(JETTY_HTTP_IDLE_TIMEOUT, Long.valueOf(v));
      }

      if (RPC_MESSAGE_MAX_SIZE.key().equalsIgnoreCase(k)) {
        set(RPC_MESSAGE_MAX_SIZE, Integer.valueOf(v));
      }

      if (RSS_CLIENT_TYPE.key().equalsIgnoreCase(k)) {
        set(RSS_CLIENT_TYPE, v);
      }

    });

    return true;
  }

}
