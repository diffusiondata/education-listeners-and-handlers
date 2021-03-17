# education-listeners-and-handlers
Working examples of the Topic Notification and Session Property listeners and the Missing Topic Notification Handler

This repo exists to support a webinar.

# Installation

`npm install`

# Execution

To start the Missing Topic Notification Handler example, serving example JSON files from directory `cdn`.

`ts-node ./src/missing-topic-handler.ts cdn`

To start the Session Property Listener example:

`ts-node ./src/session-property-listener.ts`

# Configuration

Edit `./src/serverConfig.json` to configure the server to which the tools will connect.