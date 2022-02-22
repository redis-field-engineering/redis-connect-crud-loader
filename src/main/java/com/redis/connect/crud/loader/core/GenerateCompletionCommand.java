package com.redis.connect.crud.loader.core;

import picocli.AutoComplete;
import picocli.CommandLine;

@CommandLine.Command(hidden = true, name = "generate-completion", usageHelpAutoWidth = true)
public class GenerateCompletionCommand extends AutoComplete.GenerateCompletion {
}
