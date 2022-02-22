package com.redis.connect.crud.loader;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "redis-connect-crud-loader", usageHelpAutoWidth = true, description = "CRUD loader for redis-connect with random Insert, Update and Delete events.")
public class LoaderMain extends LoaderApp {

    public static void main(String[] args) {
        log.info("Initializing CRUD Loader Application.");
        System.exit(new LoaderMain().execute(args));
    }

}