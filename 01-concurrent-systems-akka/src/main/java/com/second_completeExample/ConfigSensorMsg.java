package com.second_completeExample;

import akka.actor.ActorRef;

public class ConfigSensorMsg {
	private ActorRef dispatcher;
	
	public ConfigSensorMsg(ActorRef dispatcher) {
		this.dispatcher = dispatcher;
	}

	public ActorRef getDispatcher() {
		return dispatcher;
	}

}
