package com.galaksiya.datamanager.elasticsearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;

import com.galaksiya.logger.GLogger;

public class LoggerResponseListener {

	/**
	 * Logger instance.
	 */
	private final GLogger gLogger = new GLogger(LoggerResponseListener.class);

	private static LoggerResponseListener loggerResponseListener = new LoggerResponseListener();

	public ActionListener<BulkResponse> getLoggerResponseListener() {
		ActionListener<BulkResponse> listener = new ActionListener<>() {
			@Override
			public void onResponse(BulkResponse bulkResponse) {
				gLogger.startOperation("actionListenerOnResponse")
						.addField("responseTime", bulkResponse.getTook())
						.addField("status", bulkResponse.status().getStatus()).succeed();
			}

			@Override
			public void onFailure(Exception e) {
				gLogger.startOperation("actionListenerOnFailure").fail(e);
			}
		};

		return listener;
	}

	public static LoggerResponseListener getInstance() {
		return loggerResponseListener;
	}
}
