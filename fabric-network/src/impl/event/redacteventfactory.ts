import { RedactEventInfo } from "fabric-common";
import { RedactMessageEvent } from "../../events";
import * as util from "util";

export function newRedactMessageEvent(
  eventInfo: RedactEventInfo
): RedactMessageEvent {
  if (!eventInfo.redactBlock && !eventInfo.redactTransaction) {
    throw new Error("No redact data found: " + util.inspect(eventInfo));
  }

  const redactMessageEvent: RedactMessageEvent = {
    redactBlockData: eventInfo.redactBlock,
    redactTransactionData: eventInfo.redactTransaction,
  };

  return Object.freeze(redactMessageEvent);
}
