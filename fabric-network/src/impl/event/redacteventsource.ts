import {
  RedactEventCallback,
  RedactEventInfo,
  RedactEventListener,
  RedactEventService,
} from "fabric-common";
import { RedactMessageEvent, RedactMessageListener } from "../../events";
import * as Logger from "../../logger";
import * as GatewayUtils from "../gatewayutils";
import { AsyncNotifier } from "./asyncnotifier";
import { RedactEventServiceManager } from "./redacteventservicemanager";
import { newRedactMessageEvent } from "./redacteventfactory";
import { RedactMessageQueue } from "./redactqueue";

const logger = Logger.getLogger("RedactEventSource");

function newRedactMessageQueue(): RedactMessageQueue {
  return new RedactMessageQueue();
}

type State = "ready" | "started" | "stopped";

export class RedactMessageEventSource {
  private readonly eventServiceManager: RedactEventServiceManager;
  private eventService?: RedactEventService;
  private readonly listeners = new Set<RedactMessageListener>();
  private blockEventListener?: RedactEventListener;
  private transactionEventListener?: RedactEventListener;
  private revokeEventListener?: RedactEventListener;
  private readonly messageQueue: RedactMessageQueue;
  private readonly asyncNotifier: AsyncNotifier<RedactMessageEvent>;
  private state: State = "ready";
  private restart?: NodeJS.Immediate;

  constructor(eventServiceManager: RedactEventServiceManager) {
    this.eventServiceManager = eventServiceManager;
    this.messageQueue = newRedactMessageQueue();
    this.asyncNotifier = new AsyncNotifier(
      this.messageQueue.getNextRedactMessage.bind(this.messageQueue),
      this.notifyListeners.bind(this)
    );
  }

  async addRedactMessageListener(
    listener: RedactMessageListener
  ): Promise<RedactMessageListener> {
    this.listeners.add(listener);
    await this.start();
    return listener;
  }

  removeRedactMessageListener(listener: RedactMessageListener): void {
    this.listeners.delete(listener);
  }

  private setState(state: State) {
    if (this.state !== "stopped") {
      this.state = state;
    }
  }
  close(): void {
    this.setState("stopped");
    logger.debug("state set to  - :%s", this.state);
    this._close();
  }

  private _close(): void {
    this.unregisterListener();
    this.eventService?.close();
    this.setState("ready");
    logger.debug("state set to  - :%s", this.state);
    if (this.restart) {
      clearImmediate(this.restart);
    }
  }

  private async start(): Promise<void> {
    logger.debug("state - :%s", this.state);
    if (this.state !== "ready") {
      return;
    }
    this.state = "started";

    try {
      this.eventService = this.eventServiceManager.newDefaultEventService();
      this.registerListener(); // Register before start so no events are missed
      logger.debug("start - calling startEventService");
      await this.startEventService();
    } catch (error) {
      logger.error("Failed to start event service", error);
      this._close();
      this.restart = setImmediate(() => {
        void this.start();
      });
    }
  }

  private registerListener() {
    const callback: RedactEventCallback =
      this.redactMessageEventCallback.bind(this);
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    this.blockEventListener =
      this.eventService!.registerBlockListener(callback);
    this.transactionEventListener =
      this.eventService!.registerTransactionListener(callback);
    this.revokeEventListener =
      this.eventService!.registerRevokeListener(callback);
  }

  private unregisterListener() {
    try {
      this.blockEventListener?.unregisterEventListener();
      this.transactionEventListener?.unregisterEventListener();
      this.revokeEventListener?.unregisterEventListener();
    } catch (error) {
      logger.warn("Failed to unregister listener", error);
    }
  }

  private async startEventService() {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    await this.eventServiceManager.startEventService(this.eventService!);
  }

  private redactMessageEventCallback(error?: Error, event?: RedactEventInfo) {
    if (error) {
      this._close();
      this.restart = setImmediate(() => {
        void this.start();
      }); // Must schedule after current event loop to avoid recursion in event service notification
    } else {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      this.onRedactMessageEvent(event!);
    }
  }

  private onRedactMessageEvent(eventInfo: RedactEventInfo) {
    const redactMessageEvent = this.newRedactMessageEvent(eventInfo);
    this.messageQueue.addRedactMessage(redactMessageEvent);
    if (this.messageQueue.size() > 0) {
      this.asyncNotifier.notify();
    }
  }

  private newRedactMessageEvent(
    eventInfo: RedactEventInfo
  ): RedactMessageEvent {
    return newRedactMessageEvent(eventInfo);
  }

  private async notifyListeners(event: RedactMessageEvent) {
    const promises = Array.from(this.listeners).map((listener) =>
      listener(event)
    );
    const results = await GatewayUtils.allSettled(promises);

    for (const result of results) {
      if (result.status === "rejected") {
        logger.warn("Error notifying listener", result.reason);
      }
    }
  }
}
