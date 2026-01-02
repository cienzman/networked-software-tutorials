package com.server.txtMsg;

import akka.actor.ActorRef;

public class StartClientMsg {
	
	private ActorRef actorRef;
	
	public StartClientMsg(ActorRef actorRef) {
		this.actorRef = actorRef;
	}
	
	public ActorRef getActorRef() {
		return actorRef;
	}

}
