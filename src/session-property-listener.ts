#!/usr/bin/env ts-node
import * as diffusion from "diffusion";
import logging from "logging"

const serverConfig = require("./serverConfig.json");

/**
 * Subscribes sessions with a specic role to a given selector.
 */
class MyListener implements diffusion.SessionPropertiesListener {

    private logger: logging.Logger;

    constructor(
        private readonly session: diffusion.Session,
        private readonly role: string,
        private readonly selector: string
    ) {
        const loggerName = this.constructor.name;
        this.logger = logging('session-property-listener');
    }

    onActive() {
        this.logger.info(`SP Listener is active`)
    }

    onSessionOpen(sessionID: diffusion.SessionId, properties: diffusion.SessionProperties): void {
        this.logger.info(`Session openned ${sessionID}`, properties);
        const [rolesStr, principal] = [properties["$Roles"], properties["$Principal"]];

        if (!(rolesStr && principal)) {
            return;
        }
        const roles = new Set(diffusion.stringToRoles(rolesStr));

        if (roles.has(this.role)) {
            this.session.clients.subscribe(sessionID, this.selector).then(() => {
                this.logger.info(`Subscribed ${principal} at ${sessionID} to ${this.selector}`)
            });
        }
    }

    onSessionEvent(sessionID: diffusion.SessionId, type: diffusion.SessionEventType, properties: diffusion.SessionProperties, previous: diffusion.SessionProperties): void {
        this.logger.info(`Session changed properties ${sessionID}`, properties);
    }

    onSessionClose(sessionID: diffusion.SessionId, properties: diffusion.SessionProperties, reason: {}): void {
        this.logger.info(`Session openned ${sessionID}: ${reason}`, properties);
    }

    onClose() {
        this.logger.info(`SP Listener is closed`)
    }

    onError(error: any): void {
        this.logger.error("SP Listener error", error);
    }
}


async function main(args: string[]): Promise<void> {
    const rootLogger = logging('main');

    const session = await diffusion.connect(serverConfig);
    rootLogger.info(`Connected session ${session.sessionId} to ${serverConfig.host}` );

    await session.clients.setSessionPropertiesListener(
        ["$Principal", "$Roles"],
        new MyListener(session, "TRADER", "cdn/trader-news.json")
    );

}

main(process.argv.slice(2)).then( () => {
    // do nothing
}).catch(err => {
    console.error(err);
})