#!/usr/bin/env ts-node
import * as diffusion from "diffusion";
import chalk from "chalk"

const serverConfig = require("./serverConfig.json");

async function main(args: string[]): Promise<void> {

    if (args.length < 1) {
        console.error(`wrong # args, try <topic-subscription-path>`)
        return;
    }
    const path = args[0];

    const session = await diffusion.connect(serverConfig);

    console.log(chalk.yellowBright(`Connected session`), `${session.sessionId.toString()} to ${serverConfig.host}` );

    session
        .addStream(path, diffusion.datatypes.json())
        .on({
            subscribe: (topic, specification: diffusion.TopicSpecification) => {
                const typeName = diffusion.topics.TopicType[specification.type.id]
                console.log(chalk.yellowBright(`Subscribed to`), `${topic}, type ${typeName}` );
            },
            value: (topic, specification, newValue, oldValue) => {
                console.log(chalk.yellow(`Topic update for ${topic}: `), newValue.get());
            },
            unsubscribe: (topic, spec, reason) => {
                console.log(chalk.redBright(`Unsubscribed from ${topic}: `) + reason.reason);
            }
        });
    await session.select(path);
    console.log(chalk.yellowBright("Selected: ") + path)
}

main(process.argv.slice(2)).then( () => {
    // do nothing
}).catch(err => {
    console.error(err);
})