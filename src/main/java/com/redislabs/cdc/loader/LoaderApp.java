package com.redislabs.cdc.loader;

import com.redislabs.cdc.loader.core.GenerateCompletionCommand;
import com.redislabs.cdc.loader.core.HelpCommand;
import com.redislabs.cdc.loader.core.LoaderCommandLine;
import com.redislabs.cdc.loader.core.CRUDLoader;
import com.redislabs.cdc.loader.source.rdb.LoadRDB;
import picocli.CommandLine;

/**
 *
 * @author Virag Tripathi
 *
 */

@CommandLine.Command(sortOptions = false, subcommands = {GenerateCompletionCommand.class, CRUDLoader.class, LoadRDB.class}, abbreviateSynopsis = true)
public class LoaderApp extends HelpCommand {

    private int executionStrategy(CommandLine.ParseResult parseResult) {
        return new CommandLine.RunLast().execute(parseResult); // default execution strategy
    }

    public int execute(String... args) {
        return commandLine().execute(args);
    }

    public LoaderCommandLine commandLine() {
        LoaderCommandLine commandLine = new LoaderCommandLine(this);
        commandLine.setExecutionStrategy(this::executionStrategy);
        commandLine.setExecutionExceptionHandler(this::handleExecutionException);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        return commandLine;
    }

    private int handleExecutionException(Exception ex, CommandLine cmd, CommandLine.ParseResult parseResult) {
        // bold red error message
        cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
        return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex) : cmd.getCommandSpec().exitCodeOnExecutionException();
    }

}
