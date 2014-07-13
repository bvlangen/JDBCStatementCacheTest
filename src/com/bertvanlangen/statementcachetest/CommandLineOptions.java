package com.bertvanlangen.statementcachetest;

import com.beust.jcommander.Parameter;

public class CommandLineOptions {
    @Parameter(names = "-details", description = "With details")
    public boolean details = false;
}
