package com.tencent.rss.server;

import picocli.CommandLine.Option;

public class Arguments {

    @Option(names = {"-c", "--conf"}, description = "config file")
    private String configFile;

    public String getConfigFile() {
        return this.configFile;
    }
}
