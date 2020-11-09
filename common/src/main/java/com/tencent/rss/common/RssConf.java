package com.tencent.rss.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class with common functionality for type-safe configuration objects.
 */
public abstract class RssConf<T extends RssConf> implements Iterable<Map.Entry<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RssConf.class);
    private static final Map<String, TimeUnit> TIME_SUFFIXES;

    static {
        TIME_SUFFIXES = new HashMap<>();
        TIME_SUFFIXES.put("us", TimeUnit.MICROSECONDS);
        TIME_SUFFIXES.put("ms", TimeUnit.MILLISECONDS);
        TIME_SUFFIXES.put("s", TimeUnit.SECONDS);
        TIME_SUFFIXES.put("m", TimeUnit.MINUTES);
        TIME_SUFFIXES.put("min", TimeUnit.MINUTES);
        TIME_SUFFIXES.put("h", TimeUnit.HOURS);
        TIME_SUFFIXES.put("d", TimeUnit.DAYS);
    }

    protected final ConcurrentMap<String, String> config;
    private volatile Map<String, ConfPair> altToNewKeyMap = null;

    protected RssConf(Properties properties) {
        this.config = new ConcurrentHashMap<>();
        if (properties != null) {
            properties.stringPropertyNames().forEach(key -> {
                logDeprecationWarning(key);
                config.put(key, properties.getProperty(key));
            });
        }
    }

    public String get(String key) {
        String val = config.get(key);
        if (val != null) {
            return val;
        }
        DeprecatedConf depConf = getConfigsWithAlternatives().get(key);
        if (depConf != null) {
            return config.get(depConf.key());
        } else {
            return val;
        }
    }

    public String get(ConfEntry e) {
        Object value = get(e, String.class);
        return (String) (value != null ? value : e.dflt());
    }

    private String get(ConfEntry e, Class<?> requestedType) {
        check(getType(e.dflt()).equals(requestedType), "Invalid type conversion requested for %s.",
                e.key());
        return this.get(e.key());
    }

    @SuppressWarnings("unchecked")
    public T set(String key, String value) {
        logDeprecationWarning(key);
        config.put(key, value);
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T set(ConfEntry e, Object value) {
        check(typesMatch(value, e.dflt()), "Value doesn't match configuration entry type for %s.",
                e.key());
        if (value == null) {
            config.remove(e.key());
        } else {
            logDeprecationWarning(e.key());
            config.put(e.key(), value.toString());
        }
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setIfMissing(String key, String value) {
        if (config.putIfAbsent(key, value) == null) {
            logDeprecationWarning(key);
        }
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setAll(RssConf<?> other) {
        for (Map.Entry<String, String> e : other) {
            set(e.getKey(), e.getValue());
        }
        return (T) this;
    }

    public boolean getBoolean(ConfEntry e) {
        String val = get(e, Boolean.class);
        if (val != null) {
            return Boolean.parseBoolean(val);
        } else {
            return (Boolean) e.dflt();
        }
    }

    public int getInt(ConfEntry e) {
        String val = get(e, Integer.class);
        if (val != null) {
            return Integer.parseInt(val);
        } else {
            return (Integer) e.dflt();
        }
    }

    public long getLong(ConfEntry e) {
        String val = get(e, Long.class);
        if (val != null) {
            return Long.parseLong(val);
        } else {
            return (Long) e.dflt();
        }
    }

    public long getTimeAsMs(ConfEntry e) {
        String time = get(e, String.class);
        if (time == null) {
            check(e.dflt() != null,
                    "ConfEntry %s doesn't have a default value, cannot convert to time value.", e.key());
            time = (String) e.dflt();
        }

        Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(time.toLowerCase());
        if (!m.matches()) {
            throw new IllegalArgumentException("Invalid time string: " + time);
        }

        long val = Long.parseLong(m.group(1));
        String suffix = m.group(2);

        if (suffix != null && !TIME_SUFFIXES.containsKey(suffix)) {
            throw new IllegalArgumentException("Invalid suffix: \"" + suffix + "\"");
        }

        return TimeUnit.MILLISECONDS.convert(val,
                suffix != null ? TIME_SUFFIXES.get(suffix) : TimeUnit.MILLISECONDS);
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return config.entrySet().iterator();
    }

    protected abstract Map<String, DeprecatedConf> getConfigsWithAlternatives();

    protected abstract Map<String, DeprecatedConf> getDeprecatedConfigs();

    private boolean typesMatch(Object test, Object expected) {
        return test == null || getType(test).equals(getType(expected));
    }

    private Class<?> getType(Object o) {
        return (o != null) ? o.getClass() : String.class;
    }

    private void check(boolean test, String message, Object... args) {
        if (!test) {
            throw new IllegalArgumentException(String.format(message, args));
        }
    }

    /**
     * Logs a warning message if the given config key is deprecated.
     */
    private void logDeprecationWarning(String key) {
        ConfPair altConf = allAlternativeKeys().get(key);
        if (altConf != null) {
            LOG.warn("The configuration key " + key + " has been deprecated as of Remote Shuffle "
                    + altConf.depConf.version() + " and may be removed in the future. Please use the new key "
                    + altConf.newKey + " instead.");
            return;
        }

        DeprecatedConf depConfs = getDeprecatedConfigs().get(key);
        if (depConfs != null) {
            LOG.warn("The configuration key " + depConfs.key() + " has been deprecated as of Remote Shuffle "
                    + depConfs.version() + " and may be removed in the future. "
                    + depConfs.deprecationMessage());
        }
    }

    private Map<String, ConfPair> allAlternativeKeys() {
        if (altToNewKeyMap == null) {
            synchronized (this) {
                if (altToNewKeyMap == null) {
                    Map<String, ConfPair> configs = new HashMap<>();
                    for (String e : getConfigsWithAlternatives().keySet()) {
                        DeprecatedConf depConf = getConfigsWithAlternatives().get(e);
                        configs.put(depConf.key(), new ConfPair(e, depConf));
                    }
                    altToNewKeyMap = Collections.unmodifiableMap(configs);
                }
            }
        }
        return altToNewKeyMap;
    }

    public interface ConfEntry {

        String key();

        Object dflt();
    }

    public interface DeprecatedConf {

        String key();

        String version();

        String deprecationMessage();
    }

    private static class ConfPair {

        final String newKey;
        final DeprecatedConf depConf;

        ConfPair(String key, DeprecatedConf conf) {
            this.newKey = key;
            this.depConf = conf;
        }
    }
}
