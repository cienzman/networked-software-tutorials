package com.server;

public class PutMsg {
	private final String name;
	private final String address;


	public String getName() {
		return name;
	}


	public String getAddress() {
		return address;
	}


	public PutMsg(int id) {
		this.name = "name" + id;
		this.address = "address" + id;
	}
}


