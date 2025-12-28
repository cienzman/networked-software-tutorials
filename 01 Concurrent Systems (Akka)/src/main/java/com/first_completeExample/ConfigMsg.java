package com.first_completeExample;

import akka.actor.ActorRef;

public class ConfigMsg {
	private ActorRef actorRef;
	
	public ConfigMsg(ActorRef actorRef) {
		this.actorRef = actorRef;
	}

	public ActorRef getActorRef() {
		return actorRef;
	}
	

}
