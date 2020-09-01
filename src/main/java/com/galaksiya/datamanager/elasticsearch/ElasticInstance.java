package com.galaksiya.datamanager.elasticsearch;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonObject;

/**
 * This class creates a single elasticsearch client
 * 
 * @return
 */
public class ElasticInstance {

	private static ElasticClient elasticClient;

	public static ElasticClient getClient(String elastichost, int elasticport, Map<String, String> indexInfo,
			int shardCount, JsonObject builderProperties) {
		if (elasticClient == null) {
			createInstance(elastichost, elasticport, indexInfo, shardCount, builderProperties);
		}
		return elasticClient;
	}
	
	public static ElasticClient getClient(String elastichost, int elasticport, Map<String, String> indexInfo,
			int shardCount) {
		if (elasticClient == null) {
			createInstance(elastichost, elasticport, indexInfo, shardCount, null);
		}
		return elasticClient;
	}

	public static ElasticClient getClient(String host, int port, int shardCount, JsonObject builderProperties) {
		if (elasticClient == null) {
			createInstance(host, port, getIndexInfo(), shardCount, builderProperties);
		}
		return elasticClient;
	}

	/**
	 * This method creates elastic client connection synchronously.
	 */
	private synchronized static void createInstance(String elastichost, int elasticport, Map<String, String> indexInfo,
			int shardCount, JsonObject builderProperties) {
		if (elasticClient == null) {
			elasticClient = new ElasticClient(elastichost, elasticport, indexInfo, shardCount, builderProperties);
		}
	}

	public static Map<String, String> getIndexInfo() {
		Map<String, String> indexInfo = new HashMap<>();
		indexInfo.put("info", null);
		return indexInfo;
	}

}
