import { ListenerSession } from "./listenersession";
import { RedactMessageListener } from "../../events";
import { RedactMessageEventSource } from "./redacteventsource";

export class RedactMessageListenerSession implements ListenerSession {
  private readonly listener: RedactMessageListener;
  private readonly eventSource: RedactMessageEventSource;

  constructor(
    listener: RedactMessageListener,
    eventSource: RedactMessageEventSource
  ) {
    this.listener = listener;
    this.eventSource = eventSource;
  }

  public async start(): Promise<void> {
    await this.eventSource.addRedactMessageListener(this.listener);
  }

  public close(): void {
    this.eventSource.removeRedactMessageListener(this.listener);
    // Don't close the shared event source
  }
}
