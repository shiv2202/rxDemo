package com.galaksiya.datamanager.pulsar;

/**
 * This class creates a single pulsar client
 * 
 * @return
 */
public class PulsarInstance {
	private static IysPulsarClient pulsarClient;

	public static IysPulsarClient getIysPulsarClient(String pulsarEndpoint, String pulsarAdminEndpoint, int numPartitions) {
		if (pulsarClient == null) {
			createInstance(pulsarEndpoint, pulsarAdminEndpoint, numPartitions);
		}
		return pulsarClient;
	}
	
	/**
	 * This method creates pulsar client connection synchronously.
	 */
	private synchronized static void createInstance(String pulsarEndpoint, String pulsarAdminEndpoint, int numPartitions) {
		if (pulsarClient == null) {
			pulsarClient = new IysPulsarClient(pulsarEndpoint, pulsarAdminEndpoint, numPartitions);
		}
	}
}
