package com.tencent.rss.common.config;

import java.util.Objects;

/**
 * {@code ConfigOptions} are used to build a {@link ConfigOption}.
 * The option is typically built in one of the following pattern:
 *
 * <pre>{@code
 * // simple string-valued option with a default value
 * ConfigOption<String> tempDirs = ConfigOptions
 *     .key("tmp.dir")
 *     .stringType()
 *     .defaultValue("/tmp");
 *
 * // simple integer-valued option with a default value
 * ConfigOption<Integer> parallelism = ConfigOptions
 *     .key("application.parallelism")
 *     .intType()
 *     .defaultValue(100);
 *
 * // option with no default value
 * ConfigOption<String> userName = ConfigOptions
 *     .key("user.name")
 *     .stringType()
 *     .noDefaultValue();
 * }</pre>
 */
public class ConfigOptions {
    /**
     * Starts building a new {@link ConfigOption}.
     *
     * @param key The key for the config option.
     * @return The builder for the config option with the given key.
     */
    public static OptionBuilder key(String key) {
        Objects.requireNonNull(key);
        return new OptionBuilder(key);
    }

    // ------------------------------------------------------------------------

    /**
     * The option builder is used to create a {@link ConfigOption}.
     * It is instantiated via {@link ConfigOptions#key(String)}.
     */
    public static final class OptionBuilder {
        /** The key for the config option. */
        private final String key;

        /**
         * Creates a new OptionBuilder.
         * @param key The key for the config option
         */
        OptionBuilder(String key) {
            this.key = key;
        }

        /**
         * Defines that the value of the option should be of {@link Boolean} type.
         */
        public TypedConfigOptionBuilder<Boolean> booleanType() {
            return new TypedConfigOptionBuilder<>(key, Boolean.class);
        }

        /**
         * Defines that the value of the option should be of {@link Integer} type.
         */
        public TypedConfigOptionBuilder<Integer> intType() {
            return new TypedConfigOptionBuilder<>(key, Integer.class);
        }

        /**
         * Defines that the value of the option should be of {@link Long} type.
         */
        public TypedConfigOptionBuilder<Long> longType() {
            return new TypedConfigOptionBuilder<>(key, Long.class);
        }

        /**
         * Defines that the value of the option should be of {@link Float} type.
         */
        public TypedConfigOptionBuilder<Float> floatType() {
            return new TypedConfigOptionBuilder<>(key, Float.class);
        }

        /**
         * Defines that the value of the option should be of {@link Double} type.
         */
        public TypedConfigOptionBuilder<Double> doubleType() {
            return new TypedConfigOptionBuilder<>(key, Double.class);
        }

        /**
         * Defines that the value of the option should be of {@link String} type.
         */
        public TypedConfigOptionBuilder<String> stringType() {
            return new TypedConfigOptionBuilder<>(key, String.class);
        }
    }

    /**
     * Builder for {@link ConfigOption} with a defined atomic type.
     *
     * @param <T> atomic type of the option
     */
    public static class TypedConfigOptionBuilder<T> {
        private final String key;
        private final Class<T> clazz;

        TypedConfigOptionBuilder(String key, Class<T> clazz) {
            this.key = key;
            this.clazz = clazz;
        }

        /**
         * Creates a ConfigOption with the given default value.
         *
         * @param value The default value for the config option
         * @return The config option with the default value.
         */
        public ConfigOption<T> defaultValue(T value) {
            return new ConfigOption<>(
                    key,
                    clazz,
                    ConfigOption.EMPTY_DESCRIPTION,
                    value);
        }

        /**
         * Creates a ConfigOption without a default value.
         *
         * @return The config option without a default value.
         */
        public ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(
                    key,
                    clazz,
                    ConfigOption.EMPTY_DESCRIPTION,
                    null);
        }
    }

    // ------------------------------------------------------------------------
    /** Not intended to be instantiated. */
    private ConfigOptions() {
    }
}
