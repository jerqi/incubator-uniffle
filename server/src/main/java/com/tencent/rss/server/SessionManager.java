package com.tencent.rss.server;


import java.util.Map;

public class SessionManager {

    private Map<String, Session> sessions;

    private SessionManager() {
    }

    public static SessionManager instace() {
        return LazyHolder.INSTANCE;
    }

    public boolean init() {
        return true;
    }

    public Session getSession(String key, boolean createIfNotExist) {
        return null;
    }

    private Session createSession(String key) {
        return null;
    }

    private static class LazyHolder {

        static final SessionManager INSTANCE = new SessionManager();
    }

}






