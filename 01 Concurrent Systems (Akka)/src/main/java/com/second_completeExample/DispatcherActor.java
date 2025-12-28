package com.second_completeExample;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.AbstractActor.Receive;
import akka.japi.pf.DeciderBuilder;

public class DispatcherActor extends AbstractActorWithStash {

	// Used with load balancer
	private Map<ActorRef, ActorRef> dispatchMap;
	private Map<ActorRef, Integer> processorLoad;

	// Used with round robin
	private Iterator<ActorRef> nextProcessor;


	private final static int NO_PROCESSORS = 2;

	private static SupervisorStrategy strategy = new OneForOneStrategy(1, Duration.ofMinutes(1),
			DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.resume()).build());
	
	@Override
	public SupervisorStrategy supervisorStrategy() {
		return strategy;
	}
	
	public DispatcherActor() {
		dispatchMap = new HashMap<ActorRef, ActorRef>();
		processorLoad = new HashMap<ActorRef, Integer>();
		for (int i = 0; i < NO_PROCESSORS; i++) {
			processorLoad.put(getContext().actorOf(SensorProcessorActor.props()), 0);
		}
		nextProcessor = processorLoad.keySet().iterator();
	}


	@Override
	public AbstractActor.Receive createReceive() {
		return loadBalancer();
	}
	

	private final Receive loadBalancer() {
		return receiveBuilder()
				.match(DispatchLogicMsg.class, this::changeDispatcherLogic)
				.match(TemperatureMsg.class, this::dispatchDataLoadBalancer)
				.build();
	}

	private final Receive roundRobin() {
		return receiveBuilder()
				.match(DispatchLogicMsg.class, this::changeDispatcherLogic)
				.match(TemperatureMsg.class, this::dispatchDataRoundRobin)
				.build();
	}

	private void changeDispatcherLogic(DispatchLogicMsg msg) {
		if (msg.getLogic() == DispatchLogicMsg.LOAD_BALANCER) {
			System.out.println("DISPATCHER: Switching to load balancer!");
			getContext().become(loadBalancer());
		} else {
			System.out.println("DISPATCHER: Switching to round robin!");
			getContext().become(roundRobin());
			unstashAll();
		}
	}


	private void dispatchDataLoadBalancer(TemperatureMsg msg) {
		ActorRef sensor = msg.getSender();
		ActorRef processor = null;
		if (!dispatchMap.keySet().contains(sensor)) {
			processor = findLowLoadProcessor();
			processorLoad.put(processor, processorLoad.get(processor) + 1);
			dispatchMap.put(sensor, processor);
		}
		processor = dispatchMap.get(sensor);
		processor.tell(msg, getSelf());
	}

	private ActorRef findLowLoadProcessor() {

		// Finding the lowest load
		ActorRef lowLoadProcessor = null;
		int lowLoad = Integer.MAX_VALUE;
		for (ActorRef p : processorLoad.keySet()) {
			if (processorLoad.get(p) < lowLoad) {
				lowLoadProcessor = p;
				lowLoad = processorLoad.get(p);
			}
		}
		return lowLoadProcessor;
	}

	private void dispatchDataRoundRobin(TemperatureMsg msg) {
		if (!nextProcessor.hasNext()) {
			nextProcessor = processorLoad.keySet().iterator();
		}
		nextProcessor.next().tell(msg, self());
	}


	static Props props() {
		return Props.create(DispatcherActor.class);
	}


	public static int getNoProcessors() {
		return NO_PROCESSORS;
	}
}
