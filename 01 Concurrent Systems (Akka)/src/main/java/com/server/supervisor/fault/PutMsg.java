package com.server.supervisor.fault;

public class PutMsg {
	private final String name;
	private final String address;


	public String getName() {
		return name;
	}


	public String getAddress() {
		return address;
	}


	public PutMsg(String s) {
		this.name = "name: " + s;
		this.address = "address: " + s;
	}
}


