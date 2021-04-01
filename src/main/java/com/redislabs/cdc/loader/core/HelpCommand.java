package com.redislabs.cdc.loader.core;

import picocli.CommandLine;

@CommandLine.Command(usageHelpAutoWidth = true)
public class HelpCommand {

    @SuppressWarnings("unused")
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    private boolean helpRequested;

}