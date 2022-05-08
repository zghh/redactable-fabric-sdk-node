const TYPE = 'RedactEventService';

const BlockDecoder = require('./BlockDecoder.js');
const { checkParameter, getLogger } = require('./Utils.js');
const ServiceAction = require('./ServiceAction.js');
const RedactEventListener = require('./RedactEventListener.js');

const logger = getLogger(TYPE);

const fabproto6 = require('fabric-protos');
const { common: { Status: { SUCCESS, NOT_FOUND } } } = fabproto6;

const BLOCK = RedactEventListener.BLOCK; // for block type event listeners
const TX = RedactEventListener.TX; // for transaction type event listeners
const REVOKE = RedactEventListener.REVOKE; // for revoke type event listeners

// some info to help with debug when there are multiple eventservices running
let count = 1;
let streamCount = 1;

/**
 * EventService is used to monitor for new redact operation on a peer's ledger.
 * The class also allows the monitoring to start and end at any specific location.
 *
 * @class
 * @extends ServiceAction
 */

class RedactEventService extends ServiceAction {

    /**
     * Constructs a RedactEventService object
     *
     * @param {string} name
     * @param {Channel} channel - An instance of the Channel class
     * were this EventService will receive blocks
     * @returns {RedactEventService} An instance of this class
     */

    constructor(name = checkParameter('name'), channel = checkParameter('channel')) {
        logger.debug(`${TYPE}.constructor[${name}] - start `);
        super(name);
        this.type = TYPE;
        this.channel = channel;

        this._eventListenerRegistrations = new Map();
        this._haveRedactBlockListeners = false;
        this._haveRedactTransactionListeners = false;
        this._haveRevokeTransactionListeners = false;

        // peer's event service
        this.targets = null;
        this._currentRedactEventer = null;
        // closing state to case of multiple calls
        this._closeRunning = false;

        this.myNumber = count++;

        this.inUse = false;
    }

    /**
     * Use this method to set the ServiceEndpoint for this ServiceAction class
     * The {@link Eventer} a ServiceEndpoint must be connected before making
     * this assignment.
     * @property {Eventer[]} targets - The connected Eventer instances to
     *  be used when no targets are provided on the send.
     */
    setTargets(targets = checkParameter('targets')) {
        const method = `setTargets[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start', method);

        if (!Array.isArray(targets)) {
            throw Error('targets parameter is not an array');
        }

        if (targets.length < 1) {
            throw Error('No targets provided');
        }

        for (const eventer of targets) {
            if (eventer.isConnectable()) {
                logger.debug('%s - target is connectable %s', method, eventer.name);
            } else {
                throw Error(`Eventer ${eventer.name} is not connectable`);
            }
        }
        // must be all targets are connectable
        this.targets = targets;

        return this;
    }

    /**
     * Disconnects the RedactEventService from the fabric peer service and
     * closes all services.
     * Will close all event listeners and send an Error to all active listeners.
     */
    close() {
        const method = `close[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start - hub', method);
        this._close(new Error('RedactEventService has been shutdown by "close()" call'));
    }

    /*
     * Internal method
     * Disconnects the connection to the fabric peer service.
     * Will close all event listeners and send the provided `Error` to
     * all listeners on the event callback.
     */
    _close(reasonError = checkParameter('reasonError')) {
        const method = `_close[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start - called due to %s', method, reasonError.message);

        if (this._closeRunning) {
            logger.debug('%s - close is running - exiting', method);
            return;
        }
        this._closeRunning = true;
        this._closeAllCallbacks(reasonError);
        if (this._currentRedactEventer) {
            logger.debug('%s - have currentRedactEventer close stream %s', method, this.currentStreamNumber);
            this._currentRedactEventer.disconnect();
            this._currentRedactEventer = null;
        } else {
            logger.debug('%s - no current eventer - not shutting down stream', method);
        }

        this._closeRunning = false;
        this.inUse = false;

        logger.debug('%s - end', method);
    }

    /**
     * This method is used to build the protobuf objects.
     *
     * @param {IdentityContext} idContext - The transaction context to use for
     *  Identity, transaction ID, and nonce values
     * @returns {byte[]} The start request bytes that need to be
     *  signed.
     */
    build(idContext = checkParameter('idContext')) {
        const method = `build[${this.name}] - #${this.myNumber}`;
        logger.debug(`${method} - start`);

        this.inUse = true;

        this._payload = null;
        idContext.calculateTransactionId();

        const dataBuf = fabproto6.protos.PushRequest.encode({}).finish();

        // build the header for use with the seekInfo payload
        const channelHeaderBuf = this.channel.buildChannelHeader(
            fabproto6.common.HeaderType.REDACT_MESSAGE_QUERY,
            '',
            idContext.transactionId
        );

        const payload = fabproto6.common.Payload.create({
            header: this.buildHeader(idContext, channelHeaderBuf),
            data: dataBuf
        });
        this._payload = fabproto6.common.Payload.encode(payload).finish();

        logger.debug(`${method} - end`);
        return this._payload;
    }

    /**
     * @typedef {Object} StartRedactEventRequest
     * @property {RedactEventer[]} targets - The Eventers to send the start stream request.
     * @property {Number} [requestTimeout] - Optional. The request timeout
     */

    /**
     * This method will have this events start listening for redact operation from the
     * Peer's event service. It will send a Push request to the peer
     * event service and start the grpc streams. The received message will
     * be checked to see if there is a match to any of the registered
     * listeners.
     *
     */
    async send(request = {}) {
        const method = `send[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start', method);

        const { targets, requestTimeout } = request;
        if (targets && Array.isArray(targets) && targets.length > 0) {
            this.targets = targets;
            logger.debug('%s - using user assigned targets', method);
        } else if (this.targets) {
            logger.debug('%s - using preassigned targets', method);
        } else {
            checkParameter('targets');
        }
        const envelope = this.getSignedEnvelope();
        this._currentRedactEventer = null;
        let startError = null;

        for (const target of this.targets) {
            try {
                if (target.stream) {
                    logger.debug('%s - target has a stream, is already listening %s', method, target.toString());
                    startError = Error(`Redact event service ${target.name} is currently listening`);
                } else {
                    const isConnected = await target.checkConnection();
                    if (!isConnected) {
                        startError = Error(`Redact event service ${target.name} is not connected`);
                        logger.debug('%s - target is not connected %s', method, target.toString());
                    } else {
                        this._currentRedactEventer = await this._startService(target, envelope, requestTimeout);
                        logger.debug('%s - set current eventer %s', method, this._currentRedactEventer.toString());
                    }
                }
            } catch (error) {
                logger.error('%s - Starting stream to %s failed', method, target.name);
                startError = error;
            }

            // let see how we did with this target
            if (this._currentRedactEventer) {
                // great, it will be the one we use, stop looking
                startError = null;
                break;
            }
        }

        // if we ran through the all targets and have startError then we
        // have not found a working target endpoint, so tell user error
        if (startError) {
            logger.error('%s - no targets started - %s', method, startError);
            throw startError;
        }

        logger.debug('%s - end', method);
    }

    /*
     * internal method to startup a stream and bind this event hub's callbacks
     * to a specific target's gRPC stream
     */
    _startService(eventer, envelope, requestTimeout) {
        const me = `[${this.name}] - #${this.myNumber}`;
        const method = `_startService${me}`;
        logger.debug('%s - start', method);

        return new Promise((resolve, reject) => {
            eventer.connect();

            // the promise and streams live on and we need
            // to check at times to be sure we are working with the
            // correct one if the target gets restarted
            const stream = eventer.stream;
            const mystreamCount = streamCount++;
            this.currentStreamNumber = mystreamCount;

            logger.debug('%s - created stream %d', method, this.currentStreamNumber);

            eventer.stream.on('data', (pushResponse) => {
                logger.debug('on.data %s- peer:%s - stream:%s', me, eventer.endpoint.url, mystreamCount);
                if (stream !== eventer.stream) {
                    logger.debug('on.data %s- incoming redact operation was from a cancelled stream', me);
                    return;
                }

                logger.debug('on.data %s- resolve the promise', me);
                resolve(eventer);

                if (pushResponse.Type === 'block') {
                    try {
                        let redact_block = BlockDecoder.decodeRedactBlock(pushResponse.block);
                        logger.debug('on.data %s- incoming redact block', me);
                        this._processRedactBlockEvents(redact_block);
                    } catch (error) {
                        logger.error('on.data %s- RedactEventService - ::%s', me, error.stack);
                        logger.error('on.data %s- RedactEventService has detected an error %s', me, error);
                        // report error to all callbacks and shutdown this EventService
                        this._close(error);
                    }
                } else if (pushResponse.Type === 'transaction') {
                    try {
                        let redact_transaction = BlockDecoder.decodeRedactTransaction(pushResponse.transaction);
                        logger.debug('on.data %s- incoming redact transaction', me);
                        this._processRedactTransactionEvents(redact_transaction);
                    } catch (error) {
                        logger.error('on.data %s- RedactEventService - ::%s', me, error.stack);
                        logger.error('on.data %s- RedactEventService has detected an error %s', me, error);
                        // report error to all callbacks and shutdown this EventService
                        this._close(error);
                    }
                } else if (pushResponse.Type === 'revoke') {
                    try {
                        let revoke_transaction = BlockDecoder.decodeRevokeTransaction(pushResponse.revoke);
                        logger.debug('on.data %s- incoming revoke transaction', me);
                        this._processRevokeTransactionEvents(revoke_transaction);
                    } catch (error) {
                        logger.error('on.data %s- RedactEventService - ::%s', me, error.stack);
                        logger.error('on.data %s- RedactEventService has detected an error %s', me, error);
                        // report error to all callbacks and shutdown this EventService
                        this._close(error);
                    }
                } else if (pushResponse.Type === 'status') {
                    if (pushResponse.status === SUCCESS) {
                        logger.debug('on.data %s- received type status of SUCCESS', me);
                        logger.error('on.data %s- status SUCCESS received while redact operation are required', me);
                        this._close(new Error('Redact Event Service connection has been shutdown.'));
                    } else if (pushResponse.status === NOT_FOUND) {
                        logger.debug('on.data %s- received type status of NOT_FOUND', me);
                        logger.error('on.data %s- status SUCCESS received while redact operation are required', me);
                        this._close(new Error('Redact Event Service connection has been shutdown.'));
                    } else {
                        // tell all registered users that something is wrong and shutting down
                        logger.error('on.data %s- unexpected pushResponse status received - %s', me, pushResponse.status);
                        this._close(new Error(`Event stream has received an unexpected status message. status:${pushResponse.status}`));
                    }
                } else {
                    logger.error('on.data %s- unknown pushResponse type %s', me, pushResponse.Type);
                    this._close(new Error(`Event stream has received an unknown response type ${pushResponse.Type}`));
                }
            });

            eventer.stream.on('status', (response) => {
                logger.debug('on status %s- status received: %j  peer:%s - stream:%s', me, response, eventer.endpoint.url, mystreamCount);
            });

            eventer.stream.on('end', () => {
                logger.debug('on.end %s- peer:%s - stream:%s', me, eventer.endpoint.url, mystreamCount);
                if (stream !== eventer.stream) {
                    logger.debug('on.end %s- incoming message was from a cancelled stream', me);
                    return;
                }

                const end_error = new Error('fabric peer service has closed due to an "end" event');

                // tell all registered users that something is wrong and shutting
                // down only if this event service has been started, which means
                // that event service has an eventer endpoint assigned and this
                // service is actively listening
                if (this._currentRedactEventer) {
                    logger.debug('on.end %s- close all application listeners', me);
                    this._close(end_error);
                } else {
                    // must be we got the end while still trying to set up the
                    // listening stream, do not close the application listeners,
                    // we may try another target on the list or the application
                    // will try with another targets list
                    logger.error('on.end %s- reject the promise', me);
                    reject(end_error);
                }
            });

            eventer.stream.on('error', (err) => {
                logger.debug('on.error %s- redact peer:%s - stream:%s', me, eventer.endpoint.url, mystreamCount);
                if (stream !== eventer.stream) {
                    logger.debug('on.error %s- incoming error was from a cancelled stream - %s', me, err);
                    return;
                }

                let out_error = err;
                if (err instanceof Error) {
                    logger.debug('on.error %s- is an Error - %s', me, err);
                } else {
                    logger.debug('on.error %s- is not an Error - %s', me, err);
                    out_error = new Error(err);
                }

                // tell all registered users that something is wrong and shutting
                // down only if this event service has been started, which means
                // that event service has an eventer endpoint assigned and this
                // service is actively listening
                if (this._currentRedactEventer) {
                    logger.debug('on.error %s- close all application listeners - %s', me, out_error);
                    this._close(out_error);
                } else {
                    // must be we got the end while still trying to set up the
                    // listening stream, do not close the application listeners,
                    // we may try another target on the list or the application
                    // will try with another targets list
                    logger.error('on.error %s- reject the promise - %s', me, out_error);
                }
                reject(out_error);
            });

            try {
                eventer.stream.write(envelope);
                logger.debug('%s - stream write complete', method);
            } catch (error) {
                reject(error);
                logger.error('%s - write failed %s', method, error.stack);
            }
        });

    }

    /**
     * Use this method to indicate if application has already started using this
     * service. The service will have been asked to build the service request
     * and will not have commpleted the service startup.
     */
    isInUse() {
        const method = `isInUse[${this.name}]  - #${this.myNumber}`;
        logger.debug('%s inUse - %s', method, this.inUse);

        return this.inUse;
    }

    /**
     * Use this method to indicate if this event service has an event endpoint
     * {@link Eventer} assigned and the event endpoint has a listening stream
     * connected and active.
     */
    isStarted() {
        const method = `isStarted[${this.name}]  - #${this.myNumber}`;

        if (this._currentRedactEventer && this._currentRedactEventer.isStreamReady()) {
            logger.debug('%s - true', method);
            return true;
        } else {
            logger.debug('%s - false', method);
            return false;
        }
    }

    /**
     * Use this method to indicate if this event service has event listeners
     * {@link EventListener} assigned and waiting for an event.
     */
    hasListeners() {
        const method = `hasListeners[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start', method);

        if (this._eventListenerRegistrations.size > 0) {
            return true;
        } else {
            return false;
        }
    }

    /*
     * Internal method
     * Will close out all callbacks
     * Sends an error to all registered event callbacks
     */
    _closeAllCallbacks(err) {
        const method = `_closeAllCallbacks[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start', method);

        logger.debug('%s - event registrations %s', method, this._eventListenerRegistrations.size);
        for (const event_reg of this._eventListenerRegistrations.values()) {
            logger.debug('%s - tell listener of the error:%s', method, event_reg);
            try {
                event_reg.onEvent(err);
            } catch (error) {
                logger.error('%s - %s', method, error);
            }
        }

        logger.debug('%s - clear out the listener list', method);
        this._eventListenerRegistrations.clear();

        // all done
        logger.debug('%s - end', method);
    }

    /**
     * Unregister the event listener returned by
     * the register listener methods.
     *
     * @param {EventListener} eventListener - The registered listener.
     * @param {boolean} [notThrow] - When the listener is not found an error
     *  will be thrown when not included or false
     */
    unregisterEventListener(eventListener = checkParameter('eventListener'), notThrow) {
        const method = `unregisterEventListener[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start - eventListener:%s', method, eventListener);
        if (this._eventListenerRegistrations.has(eventListener)) {
            this._eventListenerRegistrations.delete(eventListener);
        } else {
            if (!notThrow) {
                logger.error('%s - event listener was not found', method);
                throw Error('eventListener not found');
            } else {
                logger.debug('%s - event listener was not found', method);
                return; // nothing to do
            }
        }

        let foundBlock = false;
        let foundTx = false;
        let foundRevoke = false;
        for (const event_reg of this._eventListenerRegistrations.values()) {
            if (event_reg.listenerType === BLOCK) {
                foundBlock = true;
            } else if (event_reg.listenerType === TX) {
                foundTx = true;
            } else if (event_reg.listenerType === REVOKE) {
                foundRevoke = true;
            }
        }
        this._haveRedactBlockListeners = foundBlock;
        this._haveRedactTransactionListeners = foundTx;
        this._haveRevokeTransactionListeners = foundRevoke;

        logger.debug('%s - end', method);
        return this;
    }

    /**
     * Register a listener to receive all blocks committed to this channel.
     * The listener's "callback" function gets called on the arrival of every
     * block.
     *
     * @param {EventCallback} callback
     *  for start and end block numbers or to automatically unregister
     * @returns {EventListener} The EventListener instance to be used to
     *  remove this registration using {@link EventService#unregisterEvent})
     */
    registerBlockListener(callback = checkParameter('callback')) {
        const method = `registerBlockListener[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start', method);

        const eventListener = new RedactEventListener(this, BLOCK, callback);
        this._eventListenerRegistrations.set(eventListener, eventListener);
        this._haveRedactBlockListeners = true;

        return eventListener;
    }

    /**
     * Register a callback function to receive a notification when the transaction
     * by the given id has been committed into a block. Using the special string
     * 'all' will indicate that this listener will notify (call) the callback
     * for every transaction written to the ledger.
     *
     * @param {EventCallback} callback
     * @returns {RedactEventListener} The RedactEventListener instance to be used to
     *  remove this registration using {@link EventService#unregisterEvent})
     */
    registerTransactionListener(callback = checkParameter('callback')) {
        const method = `registerTransactionListener[${this.name}] - #${this.myNumber}`;

        const eventListener = new RedactEventListener(this, TX, callback);
        this._eventListenerRegistrations.set(eventListener, eventListener);
        this._haveRedactTransactionListeners = true;

        return eventListener;
    }

    /**
     * Register a callback function to receive a notification when the transaction
     * by the given id has been committed into a block. Using the special string
     * 'all' will indicate that this listener will notify (call) the callback
     * for every transaction written to the ledger.
     *
     * @param {EventCallback} callback
     * @returns {RedactEventListener} The RedactEventListener instance to be used to
     *  remove this registration using {@link EventService#unregisterEvent})
     */
    registerRevokeListener(callback = checkParameter('callback')) {
        const method = `registerRevokeListener[${this.name}] - #${this.myNumber}`;

        const eventListener = new RedactEventListener(this, REVOKE, callback);
        this._eventListenerRegistrations.set(eventListener, eventListener);
        this._haveRevokeTransactionListeners = true;

        return eventListener;
    }

    /*
     * private internal method for processing redact block operation
     * @param {Object} redact block protobuf object
     */
    _processRedactBlockEvents(redact_block) {
        const method = `_processRedactBlockEvents[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start', method);

        if (!this._haveRedactBlockListeners) {
            logger.debug('%s - no redact block listeners', method);
            return;
        }

        for (const blockReg of this._eventListenerRegistrations.values()) {
            if (blockReg.listenerType === BLOCK) {
                logger.debug('%s - calling redact block listener callback', method);
                const event = new EventInfo(this);
                event.redactBlock = redact_block;

                try {
                    blockReg.onEvent(null, event);
                } catch (error) {
                    logger.error('%s - %s', method, error);
                }

                // check to see if we should automatically unregister
                if (blockReg.unregister) {
                    logger.debug('%s - automatically unregister redact block listener for %s', method, blockReg);
                    this.unregisterEventListener(blockReg, true);
                }
            }
        }
    }

    /*
     * private internal method for processing redact transaction operation
     * @param {Object} redact transaction protobuf object
     */
    _processRedactTransactionEvents(redact_transaction) {
        const method = `_processRedactTransactionEvents[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start', method);

        if (!this._haveRedactTransactionListeners) {
            logger.debug('%s - no redact transaction listeners', method);
            return;
        }

        for (const txReg of this._eventListenerRegistrations.values()) {
            if (txReg.listenerType === TX) {
                logger.debug('%s - calling redact transaction listener callback', method);
                const event = new EventInfo(this);
                event.redactTransaction = redact_transaction;

                try {
                    txReg.onEvent(null, event);
                } catch (error) {
                    logger.error('%s - %s', method, error);
                }

                // check to see if we should automatically unregister
                if (txReg.unregister) {
                    logger.debug('%s - automatically unregister redact transaction listener for %s', method, txReg);
                    this.unregisterEventListener(txReg, true);
                }
            }
        }
    }

    /*
     * private internal method for processing revoke transaction operation
     * @param {Object} revoke transaction protobuf object
     */
    _processRevokeTransactionEvents(revoke_transaction) {
        const method = `_processRevokeTransactionEvents[${this.name}] - #${this.myNumber}`;
        logger.debug('%s - start', method);

        if (!this._haveRevokeTransactionListeners) {
            logger.debug('%s - no revoke transaction listeners', method);
            return;
        }

        for (const txReg of this._eventListenerRegistrations.values()) {
            if (txReg.listenerType === REVOKE) {
                logger.debug('%s - calling revoke transaction listener callback', method);
                const event = new EventInfo(this);
                event.revokeTransaction = revoke_transaction;

                try {
                    txReg.onEvent(null, event);
                } catch (error) {
                    logger.error('%s - %s', method, error);
                }

                // check to see if we should automatically unregister
                if (txReg.unregister) {
                    logger.debug('%s - automatically unregister revoke transaction listener for %s', method, txReg);
                    this.unregisterEventListener(txReg, true);
                }
            }
        }
    }
}

module.exports = RedactEventService;

/**
 * @typedef {Object} EventInfo
 * @property {EventService} eventService - This EventService.
 * @property {object} [redactBlock] - The redact block received.
 * @property {object} [redactTransaction] - The redact transaction received.
 * @property {object} [revokeTransaction] - The revoke transaction received.
 */

class EventInfo {
    /**
     * Constructs a {@link EventInfo} object that contains all information about an Event.
     */
    constructor(eventService) {
        this.eventService = eventService;
        this.redactBlock;
        this.redactTransaction;
        this.revokeTransaction;
    }
}
