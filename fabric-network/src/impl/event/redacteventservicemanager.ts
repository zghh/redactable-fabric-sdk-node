/*
 * Copyright 2020 IBM All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import { Network } from "../../network";
import * as GatewayUtils from "../gatewayutils";
import {
  Channel,
  Endorser,
  RedactEventService,
  IdentityContext,
  RedactEventer,
} from "fabric-common";
import * as Logger from "../../logger";
const logger = Logger.getLogger("RedactEventSourceManager");

export class RedactEventServiceManager {
  private readonly network: Network;
  private readonly channel: Channel;
  private readonly mspId: string;
  private readonly eventServices = new Map<Endorser, RedactEventService>();
  private readonly identityContext: IdentityContext;

  constructor(network: Network) {
    this.network = network;
    this.channel = network.getChannel();
    this.mspId = network.getGateway().getIdentity().mspId;
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    this.identityContext = this.network.getGateway().identityContext!;
    logger.debug("constructor - network:%s", this.network.getChannel().name);
  }

  /**
   * Use this method to be sure the event service has been connected and has been started. If the event service is not
   * started, it will start the service based on the options provided. If the event service is already started, it
   * will check that the event service is compatible with the options provided.
   * @param eventService RedactEventService to be started if it not already started.
   */
  async startEventService(eventService: RedactEventService): Promise<void> {
    logger.debug(
      "startEventService - start %s",
      this.network.getChannel().name
    );

    eventService.build(this.identityContext);
    eventService.sign(this.identityContext);
    // targets must be previously assigned
    await eventService.send();
  }

  newDefaultEventService(): RedactEventService {
    const peers = this.getEventPeers();
    GatewayUtils.shuffle(peers);
    return this.newEventService(peers);
  }

  close(): void {
    this.eventServices.forEach((eventService) => eventService.close());
  }

  /**
   * This method will build fabric-common Eventers and the fabric-common
   * EventService. The Eventers will not be connected to the endpoint at
   * this time. Since the endorsers have been previously connected, the
   * endpoint should be accessable. The EventService will check the connection
   * and perform the connect during the send() when it starts the service.
   * @param peers The Endorser service endpoints used to build a
   *  a list of {@link Eventer} service endpoints that will be used as the
   *  targets of the new EventService.
   */
  private newEventService(peers: Endorser[]): RedactEventService {
    const serviceName = this.createName(peers);
    const eventService = this.channel.newRedactEventService(serviceName);

    const eventers = peers.map((peer) => this.newEventer(peer));
    eventService.setTargets(eventers);

    return eventService;
  }

  private newEventer(peer: Endorser): RedactEventer {
    const eventer = this.channel.client.newRedactEventer(peer.name);
    eventer.setEndpoint(peer.endpoint);
    return eventer;
  }

  private createName(peers: Endorser[]): string {
    return peers.map((peer) => peer.name).join(",");
  }

  private getEventPeers(): Endorser[] {
    const orgPeers = this.getOrganizationPeers();
    return orgPeers.length > 0 ? orgPeers : this.getNetworkPeers();
  }

  private getOrganizationPeers(): Endorser[] {
    return this.channel.getEndorsers(this.mspId);
  }

  private getNetworkPeers(): Endorser[] {
    return this.channel.getEndorsers();
  }
}
