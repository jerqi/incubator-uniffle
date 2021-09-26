package com.tencent.rss.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExitUtils {

  private static boolean isSystemExitDisabled = false;

  public static class ExitException extends RuntimeException {

    private final int status;

    ExitException(int status, String message, Throwable throwable) {
      super(message, throwable);
      this.status = status;
    }

    public int getStatus() {
      return status;
    }
  }

  /**
   *
   * @param status  exit status
   * @param message terminate message
   * @param throwable throwable caused terminate
   * @param logger  logger of the caller
   */
  public static void terminate(int status, String message, Throwable throwable, Logger logger) throws ExitException {
    if (logger != null) {
      final String s = "Terminating with exit status " + status + ": " + message;
      if (status == 0) {
        logger.info(s, throwable);
      } else {
        logger.error(s, throwable);
      }
    }

    if (!isSystemExitDisabled) {
      System.exit(status);
    }

    throw new ExitException(status, message, throwable);
  }

  public static void disableSystemExit() {
    isSystemExitDisabled = true;
  }

}
