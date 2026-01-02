package com.second_completeExample;

import java.util.concurrent.ThreadLocalRandom;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class TemperatureSensorFaultyActor extends TemperatureSensorActor {

	private ActorRef dispatcher;
	private final static int FAULT_TEMP = -50;
	
	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				.match(ConfigSensorMsg.class, this::onConfig)
				.match(GenerateMsg.class, this::onGenerate)
				.build();
	}
	
	private void onConfig(ConfigSensorMsg msg) {
		System.out.println("TEMPERATURE SENSOR: Received configuration message!");
		this.dispatcher = msg.getDispatcher();
	}

	private void onGenerate(GenerateMsg msg) {
		System.out.println("TEMPERATURE SENSOR " + self().path().name() + ": Sensing temperature!");
		dispatcher.tell(new TemperatureMsg(FAULT_TEMP,self()), self());
	}

	static Props props() {
		return Props.create(TemperatureSensorFaultyActor.class);
	}

}
