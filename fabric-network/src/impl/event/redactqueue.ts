import { RedactMessageEvent } from "../../events";

export class RedactMessageQueue {
  private queue: RedactMessageEvent[] = [];

  constructor() {}

  addRedactMessage(event: RedactMessageEvent): void {
    this.queue.push(event);
  }

  getNextRedactMessage(): RedactMessageEvent | undefined {
    if (this.queue.length == 0) {
      return;
    }
    const event = this.queue[0];
    this.queue = this.queue.slice(1);
    return event;
  }

  size(): number {
    return this.queue.length;
  }
}
