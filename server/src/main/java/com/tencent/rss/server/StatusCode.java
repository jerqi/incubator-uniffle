package com.tencent.rss.server;

public enum StatusCode {
  SUCCESS(0),
  DOUBLE_REGISTER(1),
  NO_BUFFER(2),
  INVALID_STORAGE(3),
  NO_REGISTER(4),
  NO_PARTITION(5),
  INTERNAL_ERROR(6),
  TIMEOUT(7);

  private final int statusCode;

  StatusCode(int code) {
    this.statusCode = code;
  }

  public int statusCode() {
    return statusCode;
  }
}
