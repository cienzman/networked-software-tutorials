package com.server.txtMsg;

public class TextMsg {
	private final String content;


	public String getContent() {
		return content;
	}

	public TextMsg(int id) {
		this.content = "content" + id;
	}
}


