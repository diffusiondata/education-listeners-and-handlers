#!/usr/bin/env ts-node
import * as diffusion from "diffusion";
import {readFileSync} from "fs";
import {FSWatcher, watch} from "chokidar"
import logging from "logging"

const serverConfig = require("./serverConfig.json");

/**
 * Map a topic branch to a set of paths.
 * Uses Diffusion's Topic Notification Listener
 * @param session session over which the listener is connected
 * @param path path under which the Topic Notification Listener reacts to topic events
 * @returns a live set of topics descending from `path`
 */
async function mapTopicPath(session: diffusion.Session, path: string): Promise<Set<string>> {
    const result = new Set<string>();
    const logger = logging('topic-notification-listener')
    const TopicNotificationType = session.notifications.TopicNotificationType;
    const listener: diffusion.TopicNotificationListener = {
        onTopicNotification: (path: string, _specification: diffusion.TopicSpecification, type: diffusion.TopicNotificationType) => {
            switch (type) {
                case TopicNotificationType.ADDED:
                    logger.info(`Topic ${path} has been added`);
                    result.add(path);
                    break;

                case TopicNotificationType.REMOVED:
                    logger.warn(`Topic ${path} has been removed`);
                    result.delete(path);
                    break;

                case TopicNotificationType.SELECTED:
                    logger.info(`Topic ${path} existed at the time of the selector registration.`);
                    result.add(path);
                    break;

                case TopicNotificationType.DESELECTED:
                    logger.warn(`Topic ${path} has been deselected`);
                    result.delete(path);
                    break;
            }
        },

        onDescendantNotification: (path: string, type: diffusion.TopicNotificationType) => {
            // Do nothing
        },

        onClose: () => {
            // Do nothing
        },

        onError(error: any) {
            logger.error('An error has occurred', error);
        }
    };
    const registration = await session.notifications.addListener(listener);
    const selector = `?${path}//`;
    await registration.select(selector);
    logger.info(`Watching for missing topics that match ${selector}`);

    return result;
}

class CDNHandler {

    private logger = logging('CDNHandler')

    /**
     * Default topic spec, a JSON topic with an ATR
     * policy to remove it after 1min of zero subscribers.
     */
    private static readonly defaultTopicSpec = {
        specification: new diffusion.topics.TopicSpecification(
            diffusion.topics.TopicType.JSON, {
                'REMOVAL': 'when subscriptions < 1 for 1m'
            }
        )
    }

    /**
     * Filesystem listener.
     */
    private readonly watcher: FSWatcher;

    /**
     * Build a CDNHandler
     * Async builder because constructors cannot be.
     * @param session
     * @param pathPrefix
     * @returns
     */
    static async build(session: diffusion.Session, pathPrefix: string): Promise<CDNHandler> {
        const topicSet = await mapTopicPath(session, pathPrefix);
        return new CDNHandler(session, pathPrefix, topicSet);
    }

    private constructor(
        private readonly session: diffusion.Session,
        readonly pathPrefix: string,
        readonly topics: Set<string>
    ) {
        this.watcher = watch(pathPrefix);

        // Drive changes from the filesystem into the topic tree
        this.watcher.on('unlink', async (path: string) => {
            console.log(`File ${path} unlinked`);
            if (this.topics.has(path)) {
                this.unsetTopic(path);
                console.log(`Removed topic ${path}`);
            }
        });

        this.watcher.on('change', (path: string) => {
            if (this.topics.has(path)) {
                console.log(`Updated topic ${path}`);
                this.setTopic(path);
            }
        });

    }

    /**
     * Create or update a topic, with JSON from a file of the same name.
     * Track the file for future updates.
     * @param path topic to update/create
     */
    private async setTopic(path: string): Promise<void> {
        try {
            const content = JSON.parse(readFileSync(path).toString());
            await this.session.topicUpdate.set(
                path,
                diffusion.datatypes.json(),
                content,
                CDNHandler.defaultTopicSpec);
            this.topics.add(path);
        } catch(err) {
            if (err.errno == -2) {
                this.logger.warn(`Cannot satisfy missing topic ${path}, no existing file`)
            } else {
                this.logger.error(err)
            }
        }
    }

    /**
     * Remove a topic and cease tracking it.
     * @param path topic to remove
     */
    private async unsetTopic(path: string): Promise<void> {
        await this.session.topics.remove(path);
        this.topics.delete(path);
    }

    /**
     * Handle a missing topic by attempting to create it, using the file system as the backing store.
     * @param path missing topic path
     */
    async handleMissingTopic(path: string): Promise<void> {
        try {
            this.setTopic(path)
        } catch(err) {
            console.error(err);
        }
    }
}

const rootLogger = logging('missing-topic-handler');

async function main(args: string[]): Promise<void> {
    const session = await diffusion.connect(serverConfig);

    rootLogger.info(`Connected session ${session.sessionId} to ${serverConfig.host}`);

    const cdnHandler = await CDNHandler.build(session, 'cdn');

    session.topics.addMissingTopicHandler(cdnHandler.pathPrefix, {

        onMissingTopic(notification: diffusion.MissingTopicNotification): void {
            rootLogger.info(`Missing topic notification: path=${notification.path}, selector=${notification.selector}, session=${notification.sessionID}`);
            cdnHandler.handleMissingTopic(notification.path);

            notification.proceed();
        },

        onRegister(path: string, deregister: () => void): void {
            rootLogger.info(`Registered MT notification on path ${path}`);
        },

        onClose(path: string): void {
            rootLogger.warn(`MT handler closed for path ${path}`);
        },

        onError(path: string, error: any): void {
            rootLogger.error(`MT handler for path ${path} error'd: `, error);
        }
    });
}

main(process.argv.slice(2)).then( () => {
    // do nothing
}).catch(err => {
    console.error(err);
})