package com.server.supervisor.fault;

public class GetMsg {
	private final String name;


	public String getName() {
		return name;
	}

	public GetMsg(String s) {
		this.name = "name: " + s;
	}
}