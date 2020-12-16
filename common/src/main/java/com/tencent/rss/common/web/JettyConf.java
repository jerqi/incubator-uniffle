package com.tencent.rss.common.web;

import com.tencent.rss.common.config.ConfigOption;
import com.tencent.rss.common.config.ConfigOptions;
import com.tencent.rss.common.config.RssConf;
import com.tencent.rss.common.util.RssUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JettyConf extends RssConf {

  public static final ConfigOption<Integer> JETTY_HTTP_PORT = ConfigOptions
    .key("jetty.http.port")
    .intType()
    .noDefaultValue()
    .withDescription("jetty http port");

  public static final ConfigOption<Integer> JETTY_MAX_THREAD = ConfigOptions
    .key("jetty.max.thread")
    .intType()
    .defaultValue(32)
    .withDescription("jetty max thread");

  public static final ConfigOption<Integer> JETTY_MIN_THREAD = ConfigOptions
    .key("jetty.min.thread")
    .intType()
    .defaultValue(8)
    .withDescription("jetty min thread");

  public static final ConfigOption<Integer> JETTY_HEADER_BUFFER_SIZE = ConfigOptions
    .key("jetty.header.buffer.size")
    .intType()
    .defaultValue(10 * 1024 * 1024)
    .withDescription("jetty ssl port");

  public static final ConfigOption<Boolean> JETTY_SSL_ENABLE = ConfigOptions
    .key("jetty.ssl.enable")
    .booleanType()
    .defaultValue(false)
    .withDescription("jetty ssl enable");

  public static final ConfigOption<Integer> JETTY_HTTPS_PORT = ConfigOptions
    .key("jetty.https.port")
    .intType()
    .noDefaultValue()
    .withDescription("jetty https port");

  public static final ConfigOption<String> JETTY_SSL_KEYSTORE_PATH = ConfigOptions
    .key("jetty.ssl.keystore.path")
    .stringType()
    .noDefaultValue()
    .withDescription("jetty ssl keystore path");

  public static final ConfigOption<String> JETTY_SSL_KEYMANAGER_PASSWORD = ConfigOptions
    .key("jetty.ssl.keymanager.password")
    .stringType()
    .noDefaultValue()
    .withDescription("jetty ssl keymanager password");

  public static final ConfigOption<String> JETTY_SSL_KEYSTORE_PASSWORD = ConfigOptions
    .key("jetty.ssl.keystore.password")
    .stringType()
    .noDefaultValue()
    .withDescription("jetty ssl keystore password");

  public static final ConfigOption<String> JETTY_SSL_TRUSTSTORE_PASSWORD = ConfigOptions
    .key("jetty.ssl.truststore.password")
    .stringType()
    .noDefaultValue()
    .withDescription("jetty ssl truststore password");

  public static final ConfigOption<Long> JETTY_STOP_TIMEOUT = ConfigOptions
    .key("jetty.stop.timeout")
    .longType()
    .defaultValue(30 * 1000L)
    .withDescription("jetty stop timeout (ms) ");

  public static final ConfigOption<Long> JETTY_HTTP_IDLE_TIMEOUT = ConfigOptions
    .key("jetty.http.idle.timeout")
    .longType()
    .defaultValue(30 * 1000L)
    .withDescription("jetty http idle timeout (ms) ");

  private static final Logger LOG = LoggerFactory.getLogger(JettyConf.class);

  public boolean loadConfFromFile(String fileName) {
    Map<String, String> properties = RssUtils.getPropertiesFromFile(fileName);
    if (properties == null) {
      return false;
    }

    properties.forEach((k, v) -> {
      if (JETTY_HTTP_PORT.key().equalsIgnoreCase(k)) {
        set(JETTY_HTTP_PORT, Integer.valueOf(v));
      }

      if (JETTY_MAX_THREAD.key().equalsIgnoreCase(k)) {
        set(JETTY_MAX_THREAD, Integer.valueOf(v));
      }

      if (JETTY_MIN_THREAD.key().equalsIgnoreCase(k)) {
        set(JETTY_MIN_THREAD, Integer.valueOf(v));
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

    });

    return true;
  }
}
