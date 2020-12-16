package com.tencent.rss.common;

/**
 * Base class of all rss-specific checked exceptions.
 */
public class RssException extends Exception {

  /**
   * Creates a new Exception with the given message and null as the cause.
   *
   * @param message The exception message
   */
  public RssException(String message) {
    super(message);
  }

  /**
   * Creates a new exception with a null message and the given cause.
   *
   * @param cause The exception that caused this exception
   */
  public RssException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new exception with the given message and cause.
   *
   * @param message The exception message
   * @param cause   The exception that caused this exception
   */
  public RssException(String message, Throwable cause) {
    super(message, cause);
  }
}
