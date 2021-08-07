package com.tencent.rss.common.config;

import java.util.Objects;
import java.util.function.Function;

/**
 * A {@code ConfigOption} describes a configuration parameter. It encapsulates
 * the configuration key, deprecated older versions of the key, and an optional
 * default value for the configuration parameter.
 * <p>
 * It is built via the {@code ConfigOptions} class. Once created,
 * a config option is immutable.
 *
 * @param <T> The type of value associated with the configuration option.
 */
public class ConfigOption<T> {
  static final String EMPTY_DESCRIPTION = "";

  /**
   * The current key for that config option.
   */
  private final String key;

  /**
   * The default value for this option.
   */
  private final T defaultValue;

  /**
   * The description for this option.
   */
  private final String description;

  /**
   * Type of the value that this ConfigOption describes.
   */
  private final Class<?> clazz;

  /**
   * The function which convert the input data to desired type
   */
  private final Function<Object, T> converter;

  /**
   * Creates a new config option with fallback keys.
   *
   * @param key          The current key for that config option
   * @param clazz        describes type of the ConfigOption, see description of the clazz field
   * @param description  Description for that option
   * @param defaultValue The default value for this option
   * @param converter    The method which convert the input data to desired type
   */
  ConfigOption(
      String key,
      Class<?> clazz,
      String description,
      T defaultValue,
      Function<Object, T> converter) {
    this.key = Objects.requireNonNull(key);
    this.description = description;
    this.defaultValue = defaultValue;
    this.clazz = Objects.requireNonNull(clazz);
    this.converter = Objects.requireNonNull(converter);
  }

  /**
   * Creates a new config option, using this option's key and default value, and
   * adding the given description. The given description is used when generation the configuration documentation.
   *
   * @param description The description for this option.
   * @return A new config option, with given description.
   */
  public ConfigOption<T> withDescription(final String description) {
    return new ConfigOption<>(key, clazz, description, defaultValue, converter);
  }

  // ------------------------------------------------------------------------

  /**
   * Gets the configuration key.
   *
   * @return The configuration key
   */
  public String key() {
    return key;
  }

  /**
   * Checks if this option has a default value.
   *
   * @return True if it has a default value, false if not.
   */
  public boolean hasDefaultValue() {
    return defaultValue != null;
  }

  /**
   * Returns the default value, or null, if there is no default value.
   *
   * @return The default value, or null.
   */
  public T defaultValue() {
    return defaultValue;
  }

  /**
   * Returns the class of value.
   *
   * @return The option' value class
   */
  public Class<?> getClazz() {
    return clazz;
  }

  /**
   * Returns the description of this option.
   *
   * @return The option's description.
   */
  public String description() {
    return description;
  }

  /**
   * The method which convert the input data to desired type
   * @param v      the input value
   * @param clazz  the desired type
   * @return   the value for desired type
   */
  public T convertValue(Object v, Class<?> clazz) {
    return converter.apply(v);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && o.getClass() == ConfigOption.class) {
      ConfigOption<?> that = (ConfigOption<?>) o;
      return this.key.equals(that.key)
        && (this.defaultValue == null ? that.defaultValue == null :
        (that.defaultValue != null && this.defaultValue.equals(that.defaultValue)));
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return String.format("Key: '%s' , default: %s", key, defaultValue);
  }
}
