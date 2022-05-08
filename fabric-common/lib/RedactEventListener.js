const TYPE = 'RedactEventListener';

const { checkParameter, getLogger } = require('./Utils.js');
const logger = getLogger(TYPE);

const default_unregister = {
    block: false,
    tx: false,
};

/**
 * The RedactEventListener is used internally to the EventService to hold
 * an event registration callback and settings.
 * @private
 */
class RedactEventListener {

    /**
     * Constructs a Event Listener
     * @param {EventService} eventService - The EventService where this listener is registered
     * @param {string} listenerType - a string to indicate the type of event registration
     *  "block", "tx", or "chaincode".
     * @param {function} callback - Callback for event matches
     * @param {RegExp|string} [event]
     *  <br>- When this listener is of type "block" then this field is not used.
     *  <br>- When this listener is of type "chaincode" then this
     *  field will be the chaincode event name, used as a regular
     *  expression match on the chaincode event name within the transactions.
     *  <br>- When this listener is of type "tx" then this field will be the
     *  transaction id string.
     *  In both cases this field will be compared with data in the transaction.
     *  And when there is a match the event will have taken place and the listener's callback will be called (notified).
     * @param {string} [chaincodeId] - optional. Used to isolate chaincode events
     *  to a specific chaincode.
     * @private
     */
    constructor(eventService = checkParameter('eventService'), listenerType = checkParameter('listenerType'), callback = checkParameter('callback')) {
        this.eventService = eventService;
        this.type = TYPE;
        this.listenerType = listenerType;
        this.callback = callback;
        this.unregister = default_unregister[listenerType];
    }

    /**
     * This method will be called by the {@link EventService} when it finds a
     * block that matches this event listener.
     * This method will also be called by the {@link EventService} when the
     * connection to the Peer's event service has received an error or
     * shutdown. This method will call the defined callback with the
     * event information or error instance.
     * @param {Error} error - An Error object that was created as a result
     *  of an error on the {@link EventService} connection to the Peer.
     * @param {EventInfo} event - A {@link EventInfo} that contains event information.
     * @private
     */
    onEvent(error, event) {
        const method = 'onEvent';
        try {
            this.callback(error, event);
        } catch (err) {
            logger.error('Event notification callback failed', err);
        }
    }

    /**
     * Convenience method to for users to unregister this listener
     */
    unregisterEventListener() {
        this.eventService.unregisterEventListener(this);
    }

    toString() {
        return `RedactEventListener: { listenerType: ${this.listenerType}, unregister: ${this.unregister}}`;
    }
}

module.exports = RedactEventListener;
RedactEventListener.BLOCK = 'block'; // for block type event listeners
RedactEventListener.TX = 'tx'; // for transaction type event listeners
RedactEventListener.REVOKE = 'revoke'; // for revoke type event listeners
