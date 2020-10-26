package com.tencent.rss.server;


import java.util.Map;

public class SessionManager {
  private Map<String, Session> sessions;

  private SessionManager() {}

  private static class LazyHolder {
    static final SessionManager INSTANCE = new SessionManager();
  }

  public static SessionManager instace() {
    return LazyHolder.INSTANCE;
  }

  public boolean init() {
    return true;
  }

  /**
   *
   * @param key Shuffle task key
   * @param createIfNotExist create a new session obj if not exist
   * @return the current obj or created one associated to the key
   */
  public Session getSession(String key, boolean createIfNotExist) {
    return null;
  }

  private Session createSession(String key) {
    return null;
  }

}






