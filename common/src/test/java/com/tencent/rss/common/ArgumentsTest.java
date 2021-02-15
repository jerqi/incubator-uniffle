package com.tencent.rss.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import picocli.CommandLine;

public class ArgumentsTest {

  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();

  @Test
  public void argTest() {
    String[] args = {"-c", confFile};
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    assertEquals(confFile, arguments.getConfigFile());
  }

  @Test
  public void argEmptyTest() {
    String[] args = new String[0];
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    assertNull(arguments.getConfigFile());
  }
}
