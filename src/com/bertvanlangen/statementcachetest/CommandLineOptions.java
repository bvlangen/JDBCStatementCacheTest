package com.bertvanlangen.statementcachetest;

import com.beust.jcommander.Parameter;

class CommandLineOptions {
    @Parameter(names = "-details", description = "With details")
    public boolean details = false;
}
