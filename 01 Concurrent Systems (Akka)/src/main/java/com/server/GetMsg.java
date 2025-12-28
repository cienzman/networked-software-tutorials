package com.server;

public class GetMsg {
	private final String name;


	public String getName() {
		return name;
	}

	public GetMsg(int id) {
		this.name = "name" + id;
	}
}