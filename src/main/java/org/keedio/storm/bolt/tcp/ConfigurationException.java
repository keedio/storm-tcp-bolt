package org.keedio.storm.bolt.tcp;

public class ConfigurationException extends Exception {

	private static final long serialVersionUID = 2856417661565485907L;

	public ConfigurationException() {
	}

	public ConfigurationException(String message) {
		super(message);
	}
}
