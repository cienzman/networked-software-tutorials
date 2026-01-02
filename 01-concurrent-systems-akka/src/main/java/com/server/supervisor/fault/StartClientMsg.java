package com.server.supervisor.fault;

import akka.actor.ActorRef;

public class StartClientMsg {
	private ActorRef server;

	public StartClientMsg(ActorRef server) {
		this.server = server;
	}

	public ActorRef getServer() {
		return server;
	}

}
