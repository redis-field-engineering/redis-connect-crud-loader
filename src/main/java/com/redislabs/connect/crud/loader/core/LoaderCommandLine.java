package com.redislabs.connect.crud.loader.core;

import picocli.CommandLine;

public class LoaderCommandLine extends CommandLine {

    public LoaderCommandLine(Object command) {
        super(command);
    }

    @Override
    public ParseResult parseArgs(String... args) {
        return super.parseArgs(args);
    }
}