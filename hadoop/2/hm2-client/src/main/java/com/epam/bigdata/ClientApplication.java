package com.epam.bigdata;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.cli.command.Command;
import org.springframework.yarn.boot.cli.*;
import org.springframework.yarn.boot.cli.shell.ShellCommand;

import java.util.ArrayList;
import java.util.List;

@EnableAutoConfiguration
public class ClientApplication extends AbstractCli {
    public static void main(String[] args) {
        List<Command> commands = new ArrayList<Command>();
        commands.add(new YarnPushCommand());
        commands.add(new YarnPushedCommand());
        commands.add(new YarnSubmitCommand());
        commands.add(new YarnSubmittedCommand());
        commands.add(new YarnKillCommand());
        commands.add(new YarnClustersInfoCommand());
        commands.add(new YarnClusterInfoCommand());
        commands.add(new YarnClusterCreateCommand());
        commands.add(new YarnClusterStartCommand());
        commands.add(new YarnClusterStopCommand());
        commands.add(new YarnClusterModifyCommand());
        commands.add(new YarnClusterDestroyCommand());
        ClientApplication app = new ClientApplication();
        app.registerCommands(commands);
        app.registerCommand(new ShellCommand(commands));
        app.doMain(args);
    }
}
