package com.second_completeExample;

import java.util.LinkedList;
import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class SensorProcessorActor extends AbstractActor {

	private double currentAverage;
	private List<Integer> readings;
	@Override
	public Receive createReceive() {
		return receiveBuilder().match(TemperatureMsg.class, this::gotData).build();
	}

	private void gotData(TemperatureMsg msg) throws Exception {
		System.out.println("SENSOR PROCESSOR " + self().path().name() + ": Got data from " + msg.getSender().path().name() );
		if (msg.getTemperature()<0) {
			System.out.println("SENSOR PROCESSOR " + self() + ": Failing!");
			throw new Exception("Actor fault!"); 
		}
		readings.add(msg.getTemperature());
		int sum = 0;
		for (Integer i : readings) {
			sum = sum + i;
		}
		currentAverage = sum / (double) readings.size();		
		System.out.println("SENSOR PROCESSOR " + self().path().name()  + ": Current avg is " + currentAverage);
	}

	static Props props() {
		return Props.create(SensorProcessorActor.class);
	}

	public SensorProcessorActor() {
		this.readings = new LinkedList<Integer>();
	}


}
