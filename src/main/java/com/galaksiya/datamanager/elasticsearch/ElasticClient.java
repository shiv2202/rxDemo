package com.galaksiya.datamanager.elasticsearch;

import static com.galaksiya.datamanager.utils.Constants.ErrorCodes.H100;
import static com.galaksiya.datamanager.utils.Constants.ErrorCodes.H101;
import static com.galaksiya.datamanager.utils.Constants.ErrorCodes.H102;
import static com.galaksiya.datamanager.utils.Constants.ErrorCodes.H103;
import static com.galaksiya.datamanager.utils.Constants.ErrorCodes.H174;
import static com.galaksiya.datamanager.utils.Constants.ErrorCodes.H175;
import static com.galaksiya.datamanager.utils.Constants.ErrorCodes.H177;
import static com.galaksiya.datamanager.utils.Constants.ErrorCodes.H178;
import static com.galaksiya.datamanager.utils.Constants.ErrorCodes.H179;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.AYNI_DURUM_STATUS_BILGISI_ONAY_HATASI;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.AYNI_DURUM_STATUS_BILGISI_RET_HATASI;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.FARKLI_IZIN_KAYNAGI_SOURCE_HATASI;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.ILK_IZIN_KAYIT_RET_DURUM_HATASI;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.IZIN_TARIHI_KARSILASTIRMA_HATASI;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.SIFRELEME_ISLEMLERI_YAPILAMADI_HATASI;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.SISTEMDEKI_IZNIN_TARIHINDEN_ONCEKI_TARIHLI_IZINLER_HATASI;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.SUNUCUDA_ARAMA_YAPILAMADI_HATASI;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.SUNUCUYA_ESAS_VERI_KAYDEDILEMEDI_HATASI;
import static com.galaksiya.datamanager.utils.Constants.ErrorMessages.SUNUCUYA_KURTARMA_VERISI_KAYDEDILEMEDI_HATASI;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.Level;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indexlifecycle.DeleteAction;
import org.elasticsearch.client.indexlifecycle.LifecycleAction;
import org.elasticsearch.client.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.client.indexlifecycle.Phase;
import org.elasticsearch.client.indexlifecycle.PutLifecyclePolicyRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.galaksiya.datamanager.utils.Constants;
import com.galaksiya.logger.GLogger;
import com.galaksiya.logger.OperationLog;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ElasticClient {

	public static final String APPROVAL = "approval";
	public static final String CONSENTS = "consents";
	public static final String REJECTION = "rejection";
	public static final String RETAILER = "retailer";
	public static final String TOTAL = "total";
	private static final String AFTER = "after";
	private static final String BYRETAILERS = "byretailers";
	private static final String BY_BRAND_CODE = "by_brand_code";
	private static final String BY_DAY = "by_day";
	private static final String BY_IYS_CODE = "by_iys_code";
	private static final String BY_STATUS = "by_status";
	private static final String CONSENT_COUNT = "consentCount";
	private static final String DATE = "date";
	private static final String MAIN_FIELD = "mainField";
	private static final String PROTOCOL = "http";
	private static final String QUERY_FIELD = "queryField";
	private static final String RETAILER_ACCESS_RETAIL_CODE = "retailerAccess.retailerCode";
	private static final String SIZE = "size";
	private static final String STATS = "stats";
	private static final String TOTAL_ = "TOTAL";
	private static RestClientBuilder builder;
	private BulkProcessor bulkProcessor;

	/**
	 * Logger instance.
	 */
	private final GLogger logger = new GLogger(ElasticClient.class);
	private final AtomicBoolean restarting = new AtomicBoolean(false);
	private RestHighLevelClient client;

	public boolean isClosed() {
		try {
			return !this.client.ping(RequestOptions.DEFAULT);
		} catch (Exception e) {
			return true;
		}
	}

	/**
	 * Initialize elastic client and builder. Also, create elasticsearch indices if not exist
	 */
	public ElasticClient(String host, int port, Map<String, String> indexInfo, int shardCount, JsonObject builderProperties) {
		OperationLog log = logger.startOperation("connectElasticsearch").addField("host", host).addField("port", port)
				.addField("protocol", PROTOCOL);
		try {
			builder = RestClient.builder(new HttpHost(host, port, PROTOCOL));
			builder.setDefaultHeaders(new Header[] { new BasicHeader(Constants.HEADER, Constants.VALUE) });
			getClient();
			if (builderProperties != null && !builderProperties.isJsonNull()) {
				createBulkProcessor(builderProperties);				
			}
			// create indexes in elasticsearch
			createIndices(indexInfo, shardCount);

			defineTemplateAndPolicies();

			log.succeed();
		} catch (Throwable t) {
			log.fail(t);
		}
	}
	
	/**
	 * Initialize elastic client and builder. Also, create elasticsearch indices if not exist
	 */
	public ElasticClient(String host, int port, Map<String, String> indexInfo, int shardCount) {
		OperationLog log = logger.startOperation("connectElasticsearch").addField("host", host).addField("port", port)
				.addField("protocol", PROTOCOL);
		try {
			builder = RestClient.builder(new HttpHost(host, port, PROTOCOL));
			builder.setDefaultHeaders(new Header[] { new BasicHeader(Constants.HEADER, Constants.VALUE) });
			getClient();
			// create indexes in elasticsearch
			createIndices(indexInfo, shardCount);

			defineTemplateAndPolicies();

			log.succeed();
		} catch (Throwable t) {
			log.fail(t);
		}
	}

	private void createBulkProcessor(JsonObject builderProperties) {
		BulkProcessor.Listener listener = new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {

			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				if (response.hasFailures()) {
					logger.startOperation("logBulkError").addField("description", request.getDescription())
							.addField("request", request.toString())
							.addField("Bulk [{}] executed with failures", executionId).fail();
				}
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				logger.startOperation("logBulkError").addField("description", request.getDescription()).fail(failure);
			}
		};

		BulkProcessor.Builder builder = BulkProcessor.builder(
				(request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);

		builder.setBulkActions(builderProperties.get(Constants.Bulk.BULK_ACTION_COUNT).getAsInt());
		builder.setBulkSize(new ByteSizeValue(builderProperties.get(Constants.Bulk.BULK_SIZE).getAsInt(), ByteSizeUnit.MB));
		builder.setConcurrentRequests(0);
		builder.setFlushInterval(TimeValue.timeValueSeconds(builderProperties.get(Constants.Bulk.BULK_ACTION_SECONDS).getAsInt()));
		builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));

		bulkProcessor = builder.build();

	}

	private void defineTemplateAndPolicies() {
		createTimeseriesPolicy();
		createDailyHistoryTemplate();
		createRecipientTemplate();
		createConsentTemplate();
		createHistoryTemplate();
		createAuditLogTemplate();
		// createPipeline();
		addRecipientAlias();
	}

	private void addRecipientAlias() {
		OperationLog log = logger.startOperation("addRecipientIndexAlias");
		try {
			AliasActions addIndexAlias = new AliasActions(AliasActions.Type.ADD)
					.index(Constants.ElasticUtils.recipientRealName).alias(Constants.ElasticUtils.recipientIndex);
			IndicesAliasesRequest request = new IndicesAliasesRequest();
			request.addAliasAction(addIndexAlias);
			AcknowledgedResponse indicesAliasesResponse = client.indices().updateAliases(request,
					RequestOptions.DEFAULT);
			if (indicesAliasesResponse.isAcknowledged()) {
				log.succeed();
			} else {
				log.addField("isAcknowledged", indicesAliasesResponse.isAcknowledged()).fail();
			}
		} catch (IOException e) {
			log.fail(e);
		}
	}

	// private void createPipeline() {
	// OperationLog log = logger.startOperation("createPipeline");
	// String source = "{\"description\":\"daily date-time index
	// naming\",\"processors\":[{\"date_index_name\":{\"field\":\""
	// + Constants.Audit.CREATION_DATE
	// + "\",\"index_name_prefix\":\"daily-history-\",\"date_rounding\":\"d\",\"date_formats\":[\"yyyy-MM-dd
	// HH:mm:ss\"]}}]}";
	// PutPipelineRequest request = new PutPipelineRequest("daily-history",
	// new BytesArray(source.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
	// try {
	// AcknowledgedResponse response = client.ingest().putPipeline(request, RequestOptions.DEFAULT);
	// if (response.isAcknowledged()) {
	// log.succeed();
	// } else {
	// log.fail();
	// }
	// } catch (IOException e) {
	// log.fail(e);
	// }
	// }

	private void createDailyHistoryTemplate() {
		OperationLog log = logger.startOperation("createTemplate");
		try {
			PutIndexTemplateRequest request = new PutIndexTemplateRequest("timeseries_template");
			request.patterns(List.of("daily-history-*"));
			request.settings(Settings.builder().put("index.lifecycle.name", "timeseries_policy"));
			request.mapping(Constants.ElasticUtils.dailyHistoryIndexMapping, XContentType.JSON);

			processTempleRequest(log, request);
		} catch (Exception e) {
			log.fail(e);
		}
	}

	private void createRecipientTemplate() {
		OperationLog log = logger.startOperation("createRecipientTemplate");
		try {
			PutIndexTemplateRequest request = new PutIndexTemplateRequest("recipient_template");
			request.patterns(List.of("recipient-*"));
			request.mapping(Constants.ElasticUtils.recipientIndexMapping, XContentType.JSON);

			processTempleRequest(log, request);
		} catch (Exception e) {
			log.fail(e);
		}
	}

	private void createConsentTemplate() {
		OperationLog log = logger.startOperation("createConsentTemplate");
		try {
			PutIndexTemplateRequest request = new PutIndexTemplateRequest("consent_template");
			request.patterns(List.of("consent-*"));
			request.mapping(Constants.ElasticUtils.firmIndexMapping, XContentType.JSON);

			processTempleRequest(log, request);
		} catch (Exception e) {
			log.fail(e);
		}
	}

	private void createHistoryTemplate() {
		OperationLog log = logger.startOperation("createHistoryTemplate");
		try {
			PutIndexTemplateRequest request = new PutIndexTemplateRequest("history_template");
			request.patterns(List.of("history-*"));
			request.mapping(Constants.ElasticUtils.historyIndexMapping, XContentType.JSON);

			processTempleRequest(log, request);
		} catch (Exception e) {
			log.fail(e);
		}
	}

	private void createAuditLogTemplate() {
		OperationLog log = logger.startOperation("createHistoryTemplate");
		try {
			PutIndexTemplateRequest request = new PutIndexTemplateRequest("history_template");
			request.patterns(List.of("audit-logs-*"));
			request.mapping(Constants.ElasticUtils.actionhistoryIndexMapping, XContentType.JSON);

			processTempleRequest(log, request);
		} catch (Exception e) {
			log.fail(e);
		}
	}

	private void processTempleRequest(OperationLog log, PutIndexTemplateRequest request) {
		try {
			AcknowledgedResponse putTemplateResponse = client.indices().putTemplate(request, RequestOptions.DEFAULT);
			if (putTemplateResponse.isAcknowledged()) {
				log.succeed();
			} else {
				log.fail();
			}
		} catch (IOException e) {
			log.fail(e);
		}
	}

	private void createTimeseriesPolicy() {
		OperationLog log = logger.startOperation("createTimeseriesPolicy");
		Map<String, Phase> phases = new HashMap<>();
		try {
			Map<String, LifecycleAction> deleteActions = Collections.singletonMap(DeleteAction.NAME,
					new DeleteAction());
			phases.put("delete", new Phase("delete", new TimeValue(8, TimeUnit.DAYS), deleteActions));
			LifecyclePolicy policy = new LifecyclePolicy("timeseries_policy", phases);
			PutLifecyclePolicyRequest policyRequest = new PutLifecyclePolicyRequest(policy);
			org.elasticsearch.client.core.AcknowledgedResponse response = client.indexLifecycle()
					.putLifecyclePolicy(policyRequest, RequestOptions.DEFAULT);
			boolean acknowledged = response.isAcknowledged();
			if (acknowledged) {
				log.succeed();
			} else {
				log.fail();
			}
		} catch (IOException e) {
			log.fail(e);
		}
	}

	public long addRetailerOperation(String indexName, List<String> idStringMatchList, List<String> countList,
			Map<String, Object> propertyMap, JsonObject fieldObj) throws Exception {
		String idScriptString = String.join(" || ", idStringMatchList);
		StringBuilder subString = new StringBuilder();
		for (String keyName : countList) {
			subString.append(" if (!ctx._source.retailerAccess.contains(params.").append(keyName)
					.append(")) {ctx._source.retailerAccess.add(params.").append(keyName).append(")} ");
		}
		Script script = new Script(ScriptType.INLINE, "painless",
				"if (" + idScriptString + ") {" + subString
						+ " ctx._source.putAll(params.lastModification); ctx._source.putAll(params.createdBy);}",
				propertyMap);
		BoolQueryBuilder query = createBoolQuery(fieldObj);
		UpdateByQueryRequest request = new UpdateByQueryRequest(indexName).setQuery(query);
		request.setScript(script);
		return update("addRetailer", request);
	}

	public long addRetailers(String indexName, List<String> idStringMatchList, List<String> countList,
			Map<String, Object> propertyMap, JsonObject fieldObj) throws Exception {
		return addRetailerOperation(indexName, idStringMatchList, countList, propertyMap, fieldObj);
	}

	/**
	 * Counts consents which are waiting to be saved into remote machine
	 *
	 * @param index
	 * @param fieldObj
	 * @return
	 * @throws Exception
	 */
	public JsonObject countWaitingConsents(String index, String fieldName, JsonObject fieldObj) throws Exception {
		SearchRequest searchRequest = new SearchRequest(index);
		BoolQueryBuilder finalBoolQuery = new BoolQueryBuilder();
		BoolQueryBuilder query = createBoolQueryWithObj(index, fieldObj);
		finalBoolQuery.must(query);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchSourceBuilder finalQuery = searchSourceBuilder.query(finalBoolQuery);
		ValueCountAggregationBuilder aggregation = AggregationBuilders.count(TOTAL).field(fieldName);
		finalQuery.aggregation(aggregation);

		SearchResponse searchResponse = getSearchResponse(index, searchRequest, finalQuery);

		long totalCount = 0;
		try {
			ValueCount agg = searchResponse.getAggregations().get(TOTAL);
			totalCount = agg.getValue();
		} catch (Exception e) {
			logger.startOperation("countWaitingConsents").fail(e);
		}

		JsonObject waitingResponseObj = new JsonObject();
		waitingResponseObj.addProperty(CONSENT_COUNT, totalCount);
		return waitingResponseObj;
	}

	public DateHistogramAggregationBuilder createDateHistogramAggregation(String field) {
		return AggregationBuilders.dateHistogram(BY_DAY).field(field)// .format("yyyy-MM-dd'T'HH:mm'Z'")
				.calendarInterval(DateHistogramInterval.DAY);
	}

	/**
	 * Creates boolean elastic query.
	 *
	 * @param fieldObj
	 * @return
	 */
	public BoolQueryBuilder createFinalBoolQuery(String index, JsonObject fieldObj) {
		BoolQueryBuilder finalBoolQuery = new BoolQueryBuilder();
		finalBoolQuery.must(createBoolQueryWithObj(index, fieldObj));
		return finalBoolQuery;
	}

	/**
	 * In this method, initialize any index in elasticsearch if missing.
	 */
	public void createIndex(String index, String mapping, int shardCount) {
		// create a request to check if index is already created
		boolean indexExists = isIndexAvailable(index);
		OperationLog log = logger.startOperation("CreateIndices").addField("index", index, Level.DEBUG)
				.addField("isAvailable", indexExists);
		if (!indexExists) {
			// create a request to form an index in elasticsearch
			CreateIndexRequest createIndexReq = new CreateIndexRequest(index);
			// add index mapping to request
			if (mapping != null) {
				createIndexReq.settings(Settings.builder().put("index.number_of_shards", shardCount));
				createIndexReq.mapping(mapping, XContentType.JSON);
			}
			// create index
			CreateIndexResponse createIndexResp = null;
			try {
				try {
					createIndexResp = getClient().indices().create(createIndexReq, RequestOptions.DEFAULT);
				} catch (Throwable e) {
					logger.startOperation("createIndex").fail(e);
					if (isConnectionError(e)) {
						restartClient(e);
						createIndexResp = getClient().indices().create(createIndexReq, RequestOptions.DEFAULT);
						logger.startOperation("overrideRestClient").addField("method", "createIndex")
								.addField("indexName", index, Level.DEBUG)
								.addField("indexMapping", mapping, Level.DEBUG).succeed();
					} else {
						throw e;
					}
				}
				log.succeed();
			} catch (Exception e) {
				log.fail(e);
			}
			if (createIndexResp != null && !createIndexResp.isAcknowledged()) {
				log.addField("error", String.format("Could not create index %s", index), Level.DEBUG).fail();
			}
		} else {
			log.succeed();
		}
	}

	/**
	 * This method creates index request according to given index name, document id, and data.
	 *
	 * @param indexName
	 * @param id
	 * @param data
	 * @return
	 */
	public IndexRequest createIndexRequest(String indexName, String id, String data) {
		IndexRequest insertRequest = new IndexRequest(indexName);
		if (id != null) {
			insertRequest.id(id);
		}
		insertRequest.source(data, XContentType.JSON);
		return insertRequest;
	}

	public long deleteAllRetailers(String indexName, List<String> idStringMatchList, Map<String, Object> propertyMap,
			JsonObject fieldObj) throws Exception {
		return deleteAllRetailersOperation(indexName, idStringMatchList, propertyMap, fieldObj);
	}

	public long deleteAllRetailersOperation(String indexName, List<String> idStringMatchList,
			Map<String, Object> propertyMap, JsonObject fieldObj) throws Exception {
		String idScriptString = String.join(" || ", idStringMatchList);
		String removeStrPart = "ctx._source.retailerAccess.removeIf(retailer -> true); ";
		Script script = new Script(ScriptType.INLINE, "painless",
				"if (" + idScriptString + ") " + "{" + removeStrPart
						+ " ctx._source.putAll(params.lastModification); ctx._source.putAll(params.createdBy); }",
				propertyMap);
		BoolQueryBuilder query = createBoolQuery(fieldObj);
		UpdateByQueryRequest request = new UpdateByQueryRequest(indexName).setQuery(query);
		request.setScript(script);
		return update("deleteAllRetailers", request);
	}

	public int deleteDocument(String index, String id) throws IOException {
		DeleteRequest request = new DeleteRequest(index, id);
		try {
			DeleteResponse deleteResponse = getClient().delete(request, RequestOptions.DEFAULT);
			return deleteResponse.status().getStatus();
		} catch (Throwable e) {
			logger.startOperation("deleteDocument").fail(e);
			if (isConnectionError(e)) {
				restartClient(e);
				DeleteResponse deleteResponse = getClient().delete(request, RequestOptions.DEFAULT);
				logger.startOperation("overrideRestClient").addField("method", "deleteDocument")
						.addField("indexName", index, Level.DEBUG).addField("id", id, Level.DEBUG).succeed();
				return deleteResponse.status().getStatus();
			} else {
				throw e;
			}
		}
	}

	/**
	 * This method removes given index from elasticsearch
	 *
	 * @param index Name of the index to delete.
	 * @return Returns whether the response is acknowledged or not
	 */
	public boolean deleteIndex(String index) {
		boolean isAcknowledged = false;
		OperationLog log = logger.startOperation("DeleteIndex").addField("index", index);
		try {
			DeleteIndexRequest request = new DeleteIndexRequest(index);
			try {
				AcknowledgedResponse deleteIndexResponse = getClient().indices().delete(request,
						RequestOptions.DEFAULT);
				isAcknowledged = deleteIndexResponse.isAcknowledged();
			} catch (Throwable e) {
				logger.startOperation("deleteIndex").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					AcknowledgedResponse deleteIndexResponse = getClient().indices().delete(request,
							RequestOptions.DEFAULT);
					isAcknowledged = deleteIndexResponse.isAcknowledged();
					logger.startOperation("overrideRestClient").addField("method", "deleteIndex")
							.addField("indexName", index).succeed();
				} else {
					throw e;
				}
			}
			log.succeed();
		} catch (Exception e) {
			log.addField("error", String.format("Could not delete index \"%s\"", index)).fail(e);
		}
		return isAcknowledged;
	}

	public boolean deleteIndex(RestHighLevelClient client, String indexName) {
		boolean isAcknowledged = false;
		OperationLog log = logger.startOperation("DeleteIndex").addField("index", indexName);
		try {
			DeleteIndexRequest request = new DeleteIndexRequest(indexName);
			try {
				AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
				isAcknowledged = deleteIndexResponse.isAcknowledged();
			} catch (Throwable e) {
				logger.startOperation("deleteIndex").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
					isAcknowledged = deleteIndexResponse.isAcknowledged();
					logger.startOperation("overrideRestClient").addField("method", "deleteIndex")
							.addField("indexName", indexName).succeed();
				} else {
					throw e;
				}
			}
			log.succeed();
		} catch (Exception e) {
			log.addField("error", String.format("Could not delete index \"%s\"", indexName)).fail(e);
		}
		return isAcknowledged;
	}

	public long deleteRetailer(String indexName, String script, JsonObject fieldObj) throws Exception {
		return deleteRetailerOperation(indexName, script, fieldObj);
	}

	public long deleteRetailerOperation(String indexName, String script, JsonObject fieldObj) throws Exception {
		UpdateByQueryRequest request = new UpdateByQueryRequest(indexName).setQuery(createBoolQuery(fieldObj));
		request.setScript(new Script(ScriptType.INLINE, "painless", script, Collections.emptyMap()));
		return update("deleteRetailer", request);
	}

	public long deleteRetailers(String indexName, List<String> idStringMatchList, Map<String, Object> propertyMap,
			JsonArray retailers, JsonObject fieldObj) throws Exception {
		return deleteRetailersOperation(indexName, idStringMatchList, propertyMap, retailers, fieldObj);
	}

	public long deleteRetailersOperation(String indexName, List<String> idStringMatchList,
			Map<String, Object> propertyMap, JsonArray retailers, JsonObject fieldObj) throws Exception {
		String idScriptString = String.join(" || ", idStringMatchList);
		StringBuilder removeStrPart = new StringBuilder();
		for (JsonElement retailerCodeEl : retailers) {
			removeStrPart.append(
					String.format("ctx._source.retailerAccess.removeIf(retailer -> retailer.retailerCode == \"%s\"); ",
							retailerCodeEl.getAsString(), retailerCodeEl.getAsString()));
		}

		Script script = new Script(ScriptType.INLINE, "painless",
				"if (" + idScriptString + ") " + "{" + removeStrPart
						+ " ctx._source.putAll(params.lastModification); ctx._source.putAll(params.createdBy);}",
				propertyMap);

		BoolQueryBuilder query = createBoolQuery(fieldObj);
		UpdateByQueryRequest request = new UpdateByQueryRequest(indexName).setQuery(query);
		request.setScript(script);
		return update("deleteRetailers", request);
	}

	public JsonArray getAllDocuments(String indexName) {
		MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();
		return getDocuments_(indexName, matchAllQuery);
	}

	public Map<Scroll, SearchResponse> migrateAllDocuments(String indexName) {
		MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();
		return migrateDocuments_(indexName, matchAllQuery);
	}

	/**
	 * This method returns all documents with their ids which are appropriate to the given parameters.
	 *
	 * @param indexName
	 * @param field
	 * @param domain
	 * @param fieldValueSearchPairs
	 * @param hasQueryParam
	 * @return
	 */
	public JsonArray getAllDocumentsFilterByField(String indexName, String field, String domain,
			Map<String, List<String>> fieldValueSearchPairs, boolean hasQueryParam) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
				.must(QueryBuilders.matchQuery(field, domain).operator(Operator.AND));
		if (hasQueryParam && fieldValueSearchPairs != null) {
			for (Entry<String, List<String>> entry : fieldValueSearchPairs.entrySet()) {
				List<String> values = entry.getValue();
				String[] valuesArr = new String[values.size()];
				for (int i = 0; i < values.size(); i++) {
					valuesArr[i] = values.get(i);
				}
				boolQuery
						.must(QueryBuilders.termsQuery("entity.".concat(entry.getKey()).concat(".keyword"), valuesArr));
			}
		}
		JsonArray documents = getSearchResult(indexName, field, boolQuery);
		if (documents.size() == 0) {
			boolQuery = getBoolQueryForNonKeywordField(field, domain, fieldValueSearchPairs, hasQueryParam);
			documents = getSearchResult(indexName, field, boolQuery);
		}
		return documents;
	}

	private BoolQueryBuilder getBoolQueryForNonKeywordField(String field, String domain,
			Map<String, List<String>> fieldValueSearchPairs, boolean hasQueryParam) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
				.must(QueryBuilders.matchQuery(field, domain).operator(Operator.AND));
		if (hasQueryParam && fieldValueSearchPairs != null) {
			for (Entry<String, List<String>> entry : fieldValueSearchPairs.entrySet()) {
				List<String> values = entry.getValue();
				String[] valuesArr = new String[values.size()];
				for (int i = 0; i < values.size(); i++) {
					valuesArr[i] = values.get(i);
				}
				boolQuery.must(QueryBuilders.termsQuery("entity.".concat(entry.getKey()), valuesArr));
			}
		}
		return boolQuery;
	}

	private JsonArray getSearchResult(String indexName, String field, BoolQueryBuilder boolQuery) {
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.scroll(scroll);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(boolQuery);
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = null;
		try {
			try {
				searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("getAllDocumentsFilterByField").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "getAllDocumentsFilterByField")
							.addField("indexName", indexName, Level.DEBUG).addField("field", field, Level.DEBUG)
							.succeed();
				} else {
					logger.startOperation("getAllDocumentsFilterByField").fail(e);
					throw e;
				}
			}
		} catch (Exception e1) {
			logger.error(String.format("could not search in elastic index: \"%s\"", indexName), e1);
		}
		// add search hits objects to indexDocuments array
		return scrollDocuments(scroll, searchResponse);
	}

	public RestHighLevelClient getClient() {
		if (client == null) {
			synchronized (ElasticClient.class) {
				if (client == null) {
					client = new RestHighLevelClient(builder);
				}
			}
		}
		return client;
	}

	public SearchSourceBuilder getDailyStatusChangesQuery(String index, JsonObject fieldObj, String fieldName,
			DateHistogramAggregationBuilder createDateHistogramAggregation) {
		BoolQueryBuilder finalBoolQuery = new BoolQueryBuilder();
		BoolQueryBuilder query = createBoolQueryWithObj(index, fieldObj);
		finalBoolQuery.must(query);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchSourceBuilder finalQuery = searchSourceBuilder.query(finalBoolQuery);
		TermsAggregationBuilder termAggregation = AggregationBuilders.terms(BY_STATUS).size(10000).field(fieldName);
		finalQuery.aggregation(termAggregation.subAggregation(createDateHistogramAggregation));
		return finalQuery;
	}

	/**
	 * This method retrieves all documents of given index from elasticsearch
	 *
	 * @param indexName
	 * @return
	 */
	public JsonArray getDocuments(String indexName) {
		return getDocuments_(indexName);
	}

	/**
	 * This method retrieves all documents of given index from elasticsearch
	 *
	 * @param index
	 * @return
	 */
	public JsonObject getDocumentsById(String index, String id) {
		OperationLog log = logger.startOperation("GetDocumentsById").addField("index", index, Level.DEBUG)
				.addField("documentId", id, Level.DEBUG);
		try {
			String documentStr = null;
			try {
				SearchHit[] hits = searchById(index, id);
				if (hits.length != 0) {
					documentStr = hits[0].getSourceAsString();
				}

			} catch (Throwable e) {
				logger.startOperation("getDocumentsById").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					SearchHit[] searchHits = searchById(index, id);
					if (searchHits.length != 0) {
						documentStr = searchHits[0].getSourceAsString();
					}
					logger.startOperation("overrideRestClient").addField("method", "getDocumentsById")
							.addField("index", index, Level.DEBUG).addField("documentId", id, Level.DEBUG).succeed();
				} else {
					logger.startOperation("getDocumentsById").fail(e);
					throw e;
				}
			}

			if (documentStr != null) {
				JsonElement documentEl = new Gson().fromJson(documentStr, JsonElement.class);
				log.succeed();
				return documentEl.getAsJsonObject();
			} else {
				log.addField("response", "document is not found!").succeed();
				return new JsonObject();
			}
		} catch (Exception e) {
			log.addField("error", String.format("could not get document by id! id: \"%s\"", id)).fail(e);
			JsonObject errorObj = new JsonObject();
			errorObj.addProperty("error", "exception occured");
			return errorObj;
		}
	}

	/**
	 * This method retrieves all documents of given index from elasticsearch.
	 *
	 * @param indexName
	 * @return
	 */
	public Map<Scroll, SearchResponse> migrateDocuments_(String indexName, QueryBuilder queryBuilder) {
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.scroll(scroll);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(queryBuilder);
		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = null;
		OperationLog log = logger.startOperation("GetDocuments").addField("index", indexName);
		try {
			try {
				searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("getDocuments").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "getDocuments")
							.addField("indexName", indexName)
							.addField("queryBuilder", queryBuilder.toString(), Level.DEBUG).succeed();
				} else {
					logger.startOperation("getDocuments").fail(e);
					throw e;
				}
			}
			log.succeed();
		} catch (Exception e1) {
			log.addField("error", String.format("could not search in elastic index: \"%s\"", indexName)).fail(e1);
		}
		Map<Scroll, SearchResponse> scrollResponseMap = new HashMap<Scroll, SearchResponse>();
		scrollResponseMap.put(scroll, searchResponse);
		// add search hits objects to indexDocuments array
		return scrollResponseMap; // scrollDocumentsToMigrate(scroll, searchResponse);
	}

	/**
	 * This method retrieves all documents of given index from elasticsearch.
	 *
	 * @param indexName
	 * @return
	 */
	public JsonArray getDocuments_(String indexName, QueryBuilder queryBuilder) {
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.scroll(scroll);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(queryBuilder);
		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = null;
		OperationLog log = logger.startOperation("GetDocuments").addField("index", indexName);
		try {
			try {
				searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("getDocuments").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "getDocuments")
							.addField("indexName", indexName)
							.addField("queryBuilder", queryBuilder.toString(), Level.DEBUG).succeed();
				} else {
					logger.startOperation("getDocuments").fail(e);
					throw e;
				}
			}
			log.succeed();
		} catch (Exception e1) {
			log.addField("error", String.format("could not search in elastic index: \"%s\"", indexName)).fail(e1);
		}
		// add search hits objects to indexDocuments array
		return scrollDocuments(scroll, searchResponse);
	}

	public JsonArray getDocuments_(String indexName) {
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.scroll(scroll);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = null;
		try {
			try {
				searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("getDocuments").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "getDocuments")
							.addField("indexName", indexName).succeed();
				} else {
					logger.startOperation("getDocuments").fail(e);
					throw e;
				}
			}
		} catch (Exception e1) {
			logger.error(String.format("could not search in elastic index: \"%s\"", indexName), e1);
		}
		JsonArray indexDocuments = new JsonArray();
		if (searchResponse != null) {
			String scrollId = searchResponse.getScrollId();
			SearchHit[] searchHits = getDocuments(searchResponse, indexDocuments);

			while (searchHits != null && searchHits.length > 0) {
				SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
				scrollRequest.scroll(scroll);
				try {
					try {
						searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
					} catch (Throwable e) {
						logger.startOperation("getDocuments").fail(e);
						if (isConnectionError(e)) {
							restartClient(e);
							searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
							logger.startOperation("overrideRestClient").addField("method", "getDocuments")
									.addField("indexName", indexName).succeed();
						} else {
							logger.startOperation("getDocuments").fail(e);
							throw e;
						}
					}
				} catch (Exception e) {
					logger.error(String.format("could not scroll! scroll id: \"%s\"", scrollId), e);
				}
				scrollId = searchResponse.getScrollId();
				searchHits = searchResponse.getHits().getHits();
				for (SearchHit hit : searchHits) {
					JsonObject documentObj = new Gson().fromJson(hit.toString(), JsonObject.class)
							.get(Constants._SOURCE).getAsJsonObject();
					indexDocuments.add(documentObj);
				}
			}
			ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
			clearScrollRequest.addScrollId(scrollId);
			try {
				try {
					getClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
				} catch (Throwable e) {
					logger.startOperation("getDocuments").fail(e);
					if (isConnectionError(e)) {
						restartClient(e);
						getClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
						logger.startOperation("overrideRestClient").addField("method", "getDocuments")
								.addField("indexName", indexName).succeed();
					} else {
						logger.startOperation("getDocuments").fail(e);
						throw e;
					}
				}
			} catch (Exception e) {
				logger.error("could not clear scroll", e);
			}
		}
		return indexDocuments;
	}

	public JsonObject getErrorBase(String code, String message) {
		JsonObject errorObj = new JsonObject();
		errorObj.addProperty("code", code);
		errorObj.addProperty("message", message);
		return errorObj;
	}

	public JsonObject getErrorResponse(String code, String message) {
		JsonObject responseObj = new JsonObject();
		JsonArray errorResponse = new JsonArray();
		JsonObject errorObj = getErrorBase(code, message);
		errorResponse.add(errorObj);
		responseObj.add("errors", errorResponse);
		return responseObj;
	}

	public IndexRequest getIndexRequest(String indexName, String id, String data) {
		return createIndexRequest(indexName, id, data);
	}

	public JsonArray getLastDocuments(String indexName, JsonArray shouldList) throws Exception {
		JsonArray indexDocuments = new JsonArray();
		for (JsonElement shouldElement : shouldList) {
			SearchRequest searchRequest = new SearchRequest(indexName);
			// BoolQueryBuilder finalBoolQuery = new BoolQueryBuilder();
			// BoolQueryBuilder query = createBoolQueryWithObj(indexName, shouldElement.getAsJsonObject());
			// finalBoolQuery.must(query);
			// SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			// FieldSortBuilder creationDateSort = SortBuilders.fieldSort(Constants.Audit.CREATION_DATE)
			// .order(SortOrder.DESC);
			SearchSourceBuilder searchSourceBuilder = createBoolQueryForLastConsents(indexName,
					shouldElement.getAsJsonObject());
			SearchSourceBuilder finalQuery = searchSourceBuilder.from(0).size(1);

			// add term aggregation
			SearchResponse searchResponse = getSearchResponse(indexName, searchRequest, finalQuery);
			if (searchResponse != null) {
				SearchHit[] searchHits = searchResponse.getHits().getHits();
				// add search hits objects to indexDocuments array
				indexDocuments.addAll(getSearchHits(searchHits));
			}
		}
		return indexDocuments;
	}

	/**
	 * This method retrieves all documents of given index from elasticsearch.
	 *
	 * @param indexName
	 * @return
	 */
	public Map<Scroll, SearchResponse> searchRecipientDocuments(String indexName, JsonObject fieldObj) {
		MatchAllQueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.scroll(scroll);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		BoolQueryBuilder finalBoolQuery = new BoolQueryBuilder();
		BoolQueryBuilder query = createBoolQueryWithObj(indexName, fieldObj);
		finalBoolQuery.must(query);

		searchSourceBuilder.query(finalBoolQuery);
		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = null;
		OperationLog log = logger.startOperation("GetDocuments").addField("index", indexName);
		try {
			try {
				searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("getDocuments").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "getDocuments")
							.addField("indexName", indexName)
							.addField("queryBuilder", queryBuilder.toString(), Level.DEBUG).succeed();
				} else {
					logger.startOperation("getDocuments").fail(e);
					throw e;
				}
			}
			log.succeed();
		} catch (Exception e1) {
			log.addField("error", String.format("could not search in elastic index: \"%s\"", indexName)).fail(e1);
		}
		Map<Scroll, SearchResponse> scrollResponseMap = new HashMap<Scroll, SearchResponse>();
		scrollResponseMap.put(scroll, searchResponse);
		// add search hits objects to indexDocuments array
		return scrollResponseMap; // scrollDocumentsToMigrate(scroll, searchResponse);
	}

	/**
	 * This method retrieves all documents of given index from elasticsearch.
	 *
	 * @param indexName
	 * @return
	 */
	public Map<Scroll, SearchResponse> searchDocumentsWithinTimeInterval(String indexName, String startTime,
			String endTime, int scrollSize) {
		MatchAllQueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.scroll(scroll);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		BoolQueryBuilder query = QueryBuilders.boolQuery();
		query.must(QueryBuilders.rangeQuery(Constants.Audit.CREATION_DATE).gte(startTime).lte(endTime));

		searchSourceBuilder.query(query).size(scrollSize);
		// ValueCountAggregationBuilder aggregation = AggregationBuilders.count("agg").field("brandCode");
		// searchSourceBuilder.aggregation(aggregation);

		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = null;
		OperationLog log = logger.startOperation("GetDocuments").addField("index", indexName);
		try {
			try {
				searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("getDocuments").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "getDocuments")
							.addField("indexName", indexName)
							.addField("queryBuilder", queryBuilder.toString(), Level.DEBUG).succeed();
				} else {
					logger.startOperation("getDocuments").fail(e);
					throw e;
				}
			}
			log.succeed();
		} catch (Exception e1) {
			log.addField("error", String.format("could not search in elastic index: \"%s\"", indexName)).fail(e1);
		}
		Map<Scroll, SearchResponse> scrollResponseMap = new HashMap<Scroll, SearchResponse>();
		scrollResponseMap.put(scroll, searchResponse);
		// add search hits objects to indexDocuments array
		return scrollResponseMap; // scrollDocumentsToMigrate(scroll, searchResponse);
	}
	
	/**
	 * This method retrieves all documents of given index from elasticsearch.
	 *
	 * @param indexName
	 * @return
	 */
	public Map<Scroll, SearchResponse> searchDocumentsForMerging(String indexName, int scrollSize) {
		MatchAllQueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(indexName);
		searchRequest.scroll(scroll);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		BoolQueryBuilder query = QueryBuilders.boolQuery();
		query.must(QueryBuilders.matchAllQuery());

		searchSourceBuilder.query(query).size(scrollSize);
		// ValueCountAggregationBuilder aggregation = AggregationBuilders.count("agg").field("brandCode");
		// searchSourceBuilder.aggregation(aggregation);

		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = null;
		OperationLog log = logger.startOperation("GetDocuments").addField("index", indexName);
		try {
			try {
				searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("getDocuments").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "getDocuments")
							.addField("indexName", indexName)
							.addField("queryBuilder", queryBuilder.toString(), Level.DEBUG).succeed();
				} else {
					logger.startOperation("getDocuments").fail(e);
					throw e;
				}
			}
			log.succeed();
		} catch (Exception e1) {
			log.addField("error", String.format("could not search in elastic index: \"%s\"", indexName)).fail(e1);
		}
		Map<Scroll, SearchResponse> scrollResponseMap = new HashMap<Scroll, SearchResponse>();
		scrollResponseMap.put(scroll, searchResponse);
		// add search hits objects to indexDocuments array
		return scrollResponseMap; // scrollDocumentsToMigrate(scroll, searchResponse);
	}

	public SearchSourceBuilder getSearchConsentsQuery(String indexName, JsonObject queryExtensions,
			JsonObject fieldObj) {
		BoolQueryBuilder finalBoolQuery = new BoolQueryBuilder();
		BoolQueryBuilder query = createBoolQueryWithObj(indexName, fieldObj);
		if (fieldObj.has(Constants.Consent.RETAILER_CODE)) {
			query.must(QueryBuilders.nestedQuery("retailerAccess",
					QueryBuilders.matchQuery(RETAILER_ACCESS_RETAIL_CODE,
							fieldObj.get(Constants.Consent.RETAILER_CODE).getAsString()).operator(Operator.AND),
					ScoreMode.Total));
		}
		finalBoolQuery.must(query);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchSourceBuilder sourceBuilder = searchSourceBuilder.from(queryExtensions.get(AFTER).getAsInt())
				.size(queryExtensions.get(SIZE).getAsInt());
		if (indexName.equals(Constants.ElasticUtils.consentIndex)) {
			FieldSortBuilder idSort = SortBuilders.fieldSort(Constants._ID).order(SortOrder.DESC);
			FieldSortBuilder creationDateSort = SortBuilders
					.fieldSort(queryExtensions.get(Constants.Audit.FIRST_CREATION_DATE).getAsString())
					.order(SortOrder.DESC);
			sourceBuilder = sourceBuilder.sort(creationDateSort).sort(idSort);
		}
		SearchSourceBuilder builder = sourceBuilder.query(finalBoolQuery);
		return builder;
	}

	/**
	 * This method creates a term aggregation object.
	 *
	 * @param aggName
	 * @param fieldName
	 * @return
	 */
	public TermsAggregationBuilder getTermAggregation(String aggName, String fieldName) {
		return AggregationBuilders.terms(aggName).size(10000).field(fieldName);
	}

	public boolean indexAvailable(String indexName) {
		boolean indexExists = false;
		OperationLog log = logger.startOperation("IsIndexAvailable").addField("index", indexName);
		try {
			GetIndexRequest indexRequest = new GetIndexRequest(indexName);
			try {
				indexExists = getClient().indices().exists(indexRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("isIndexAvailable").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					indexExists = getClient().indices().exists(indexRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "isIndexAvailable")
							.addField("indexName", indexName).succeed();
				} else {
					logger.startOperation("getFromIndex").fail(e);
					throw e;
				}
			}
			log.addField("indexExists", indexExists).succeed();
		} catch (Exception e) {
			log.addField("error", String.format("Could not check if index %s exists.", indexName)).fail(e);
		}
		return indexExists;
	}

	/**
	 * This method is used to index action history logs as a sync bulk. It sends bulk request to elastic search then if
	 * the bulk messages has some failures. Failure messages are added to operation log.
	 * 
	 * @param bulkRequest it is the bulk request that holds all the requests which will be index to elasticsearch.
	 */
	public void indexBulkRequest(BulkRequest bulkRequest) {
		OperationLog log = logger.startOperation("indexBulkRequest", Level.DEBUG);
		try {
			BulkResponse bulkResponse = getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
			if (bulkResponse.hasFailures()) {
				int index = 0;
				for (BulkItemResponse bulkItemResponse : bulkResponse) {
					if (bulkItemResponse.isFailed()) {
						BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
						log.addField(String.format("item[%s]", index), failure.getMessage(), Level.DEBUG);
					}
					index += 1;
				}
				log.fail();
			} else {
				log.succeed();
			}
		} catch (Exception e) {
			log.fail(e);
		}
	}

	/**
	 * This method saves given data to given index in elasticsearch.
	 *
	 * @param indexName
	 * @param data
	 * @return
	 * @throws IOException
	 */
	public String indexData(String indexName, String id, String data) throws IOException {
		return insert(getIndexRequest(indexName, id, data));
	}

	public void addToBulkProcessor(IndexRequest request) {
		bulkProcessor.add(request);	
	}
	
	public String insert(IndexRequest insertRequest) throws IOException {
		OperationLog log = logger.startOperation("indexDocument");
		try {
			String id = getClient().index(insertRequest, RequestOptions.DEFAULT).getId();
			log.addField("indexedDocumentId", id, Level.DEBUG).succeed();
			return id;
		} catch (Throwable e) {
			logger.startOperation("insert").fail(e);
			if (isConnectionError(e)) {
				log.addField("connection", "restoring");
				restartClient(e);
				log.addField("connection", "restored");
				String id = getClient().index(insertRequest, RequestOptions.DEFAULT).getId();
				log.addField("indexedDocumentId", id, Level.DEBUG).succeed();
				return id;
			} else {
				log.fail(e);
				throw e;
			}
		}
	}

	/**
	 * This method checks if given index is already created.
	 *
	 * @param index name of the to check the availability.
	 * @return
	 */
	public boolean isIndexAvailable(String index) {
		return indexAvailable(index);
	}

	//

	public long overrideRetailers(String indexName, List<String> idStringMatchList, List<String> countList,
			Map<String, Object> propertyMap, JsonObject fieldObj) throws Exception {
		return overrideRetailersOperation(indexName, idStringMatchList, countList, propertyMap, fieldObj);
	}

	public long overrideRetailersOperation(String indexName, List<String> idStringMatchList, List<String> countList,
			Map<String, Object> propertyMap, JsonObject fieldObj) throws Exception {
		String idScriptString = String.join(" || ", idStringMatchList);
		StringBuilder subString = new StringBuilder();
		for (String keyName : countList) {
			subString.append(" if (!ctx._source.retailerAccess.contains(params.").append(keyName)
					.append(")) {ctx._source.retailerAccess.add(params.").append(keyName).append(")} ");
		}
		Script script = new Script(ScriptType.INLINE, "painless",
				"if (" + idScriptString + ") { ctx._source.retailerAccess.removeIf(retailer -> true); " + subString
						+ " ctx._source.putAll(params.lastModification); ctx._source.putAll(params.createdBy); }",
				propertyMap);
		BoolQueryBuilder query = createBoolQuery(fieldObj);
		UpdateByQueryRequest request = new UpdateByQueryRequest(indexName).setQuery(query);
		request.setScript(script);
		return update("overrideRestClient", request);
	}

	public void refresh(String index) {
		RefreshRequest refreshRequest = new RefreshRequest(index);
		OperationLog log = logger.startOperation("RefreshIndex").addField("index", index);
		try {
			try {
				getClient().indices().refresh(refreshRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("refreshIndex").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					getClient().indices().refresh(refreshRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "refreshIndex")
							.addField("indexName", index).succeed();
				} else {
					throw e;
				}
			}
			log.succeed();
		} catch (Exception e) {
			log.addField("error", String.format("could not refresh index: \"%s\"", index)).fail(e);
		}
	}

	/**
	 * This method refreshes given index.
	 *
	 * @param index name of the index to refresh
	 */
	public void refreshIndex(String index) {
		refresh(index);

	}

	/**
	 * This method searches for documents which have field values that are given through fieldObj. Then it returns found
	 * documents as a json array.
	 *
	 * @param index
	 * @param fieldObj
	 * @return
	 * @throws Exception
	 */
	public JsonArray searchBooleanWithFields(String index, JsonObject fieldObj) throws Exception {
		Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(index);
		searchRequest.scroll(scroll);
		BoolQueryBuilder finalBoolQuery = createFinalBoolQuery(index, fieldObj);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(finalBoolQuery);

		SearchResponse searchResponse = getSearchResponse(index, searchRequest, searchSourceBuilder);

		return scrollDocuments(scroll, searchResponse);
	}

	/**
	 * This method searches for documents which have field values that are given through fieldObj. Then it returns found
	 * documents as a json array.
	 *
	 * @param index
	 * @param fieldObj
	 * @return
	 * @throws Exception
	 */
	public JsonArray searchMessages(String index, JsonObject fieldObj) throws Exception {
		Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(index);
		searchRequest.scroll(scroll);
		BoolQueryBuilder finalBoolQuery = createFinalBoolQuery(index, fieldObj);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(finalBoolQuery);
		searchSourceBuilder.aggregation(getTermAggregation("bysubrequestid", "subRequestId")
				.subAggregation(getTermAggregation("bystatuscode", "statusCode")
						.subAggregation(getTermAggregation("byactioncode", "actionCode")
								.subAggregation(AggregationBuilders.topHits("bytops")))));

		SearchResponse searchResponse = getSearchResponse(index, searchRequest, searchSourceBuilder);
		Terms subRequestIdTerms = searchResponse.getAggregations().get("bysubrequestid");
		JsonArray results = new JsonArray();
		for (Terms.Bucket subRequestIdBucket : subRequestIdTerms.getBuckets()) {
			Terms bystatusTerms = subRequestIdBucket.getAggregations().get("bystatuscode");
			int maxStatusCode = 0;
			Terms.Bucket maxTermBucket = null;
			for (Terms.Bucket bystatusBucket : bystatusTerms.getBuckets()) {
				if (bystatusBucket.getKeyAsNumber().intValue() > maxStatusCode) {
					maxStatusCode = bystatusBucket.getKeyAsNumber().intValue();
					maxTermBucket = bystatusBucket;
				}
			}
			Terms byactioncodeTerms = maxTermBucket.getAggregations().get("byactioncode");
			int maxActionCode = 0;
			Terms.Bucket maxActionCodeBucket = null;
			for (Terms.Bucket byactioncodeBucket : byactioncodeTerms.getBuckets()) {
				if (byactioncodeBucket.getKeyAsNumber().intValue() > maxActionCode) {
					maxActionCode = byactioncodeBucket.getKeyAsNumber().intValue();
					maxActionCodeBucket = byactioncodeBucket;
				}
			}

			TopHits topHits = maxActionCodeBucket.getAggregations().get("bytops");
			for (SearchHit hit : topHits.getHits().getHits()) {
				results.add(new Gson().fromJson(hit.getSourceAsString(), JsonObject.class));
			}

		}

		return results;
	}

	/**
	 * This method adds the error message to error object.
	 * 
	 * @param messageObj
	 * @param errorObj
	 */
	public String decideErrorMessage(String code, String message) {
		String translatedMessage = null;
		switch (code) {
		case H174:
			if (message.contains("ONAY")) {
				translatedMessage = AYNI_DURUM_STATUS_BILGISI_ONAY_HATASI;
			} else {
				translatedMessage = AYNI_DURUM_STATUS_BILGISI_RET_HATASI;
			}
			break;
		case H175:
			translatedMessage = ILK_IZIN_KAYIT_RET_DURUM_HATASI;
			break;
		case H102:
			translatedMessage = SIFRELEME_ISLEMLERI_YAPILAMADI_HATASI;
			break;
		case H100:
			translatedMessage = SUNUCUYA_KURTARMA_VERISI_KAYDEDILEMEDI_HATASI;
			break;
		case H101:
			translatedMessage = SUNUCUYA_ESAS_VERI_KAYDEDILEMEDI_HATASI;
			break;
		case H177:
			translatedMessage = FARKLI_IZIN_KAYNAGI_SOURCE_HATASI;
			break;
		case H178:
			translatedMessage = SISTEMDEKI_IZNIN_TARIHINDEN_ONCEKI_TARIHLI_IZINLER_HATASI;
			break;
		case H179:
			translatedMessage = IZIN_TARIHI_KARSILASTIRMA_HATASI;
			break;
		case H103:
			translatedMessage = SUNUCUDA_ARAMA_YAPILAMADI_HATASI;
			break;
		}
		return translatedMessage;
	}

	/**
	 * This method is used to search brands with aggregations in elasticsearch.
	 *
	 * @param indexName
	 * @param properties
	 * @return
	 * @throws Exception
	 */
	public JsonArray searchBrands(String indexName, JsonArray properties) {
		SearchRequest searchRequest = new SearchRequest(indexName);
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		for (JsonElement propertyEl : properties) {
			JsonObject hashObj = propertyEl.getAsJsonObject().get("hash").getAsJsonObject();
			BoolQueryBuilder subQuery = QueryBuilders.boolQuery();
			for (Entry<String, JsonElement> key : hashObj.entrySet()) {
				subQuery.must(
						QueryBuilders.matchQuery(key.getKey(), key.getValue().getAsString()).operator(Operator.AND));
			}
			query.should(subQuery);
		}
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchSourceBuilder finalQuery = searchSourceBuilder.query(query);
		AggregationBuilder aggregation = AggregationBuilders.nested(BYRETAILERS, Constants.Consent.RETAILER_ACCESS)
				.subAggregation(AggregationBuilders.terms(Constants.Consent.RETAILER_CODE).size(10000)
						.field(RETAILER_ACCESS_RETAIL_CODE));
		// add term aggregation
		finalQuery.aggregation(getTermAggregation(BY_BRAND_CODE, Constants.Consent.BRAND_CODE)
				.subAggregation(getTermAggregation(BY_IYS_CODE, Constants.Consent.IYS_CODE)
						.subAggregation(getTermAggregation(BY_STATUS, Constants.Consent.STATUS))
						.subAggregation(aggregation)));
		SearchResponse searchResponse;
		try {
			searchResponse = getSearchResponse(indexName, searchRequest, finalQuery);
			JsonArray searchResponseList = new JsonArray();
			if (searchResponse != null) {
				Terms agg = searchResponse.getAggregations().get(BY_BRAND_CODE);
				for (Terms.Bucket entry : agg.getBuckets()) {
					JsonObject aggregationResult = new JsonObject();
					aggregationResult.addProperty(Constants.Consent.BRAND_CODE, entry.getKey().toString());
					Terms iysCodeTerms = entry.getAggregations().get(BY_IYS_CODE);
					for (Terms.Bucket iysCodeBucket : iysCodeTerms.getBuckets()) {
						Terms statusTerms = iysCodeBucket.getAggregations().get(BY_STATUS);
						JsonObject stats = new JsonObject();
						JsonObject consents = new JsonObject();
						consents.addProperty(APPROVAL, 0);
						consents.addProperty(REJECTION, 0);
						long total = 0;
						for (Terms.Bucket statusBucket : statusTerms.getBuckets()) {
							long docCount = statusBucket.getDocCount();
							consents.addProperty("ONAY".equals(statusBucket.getKey().toString()) ? APPROVAL : REJECTION,
									docCount);
							total += docCount;
						}
						consents.addProperty(TOTAL, total);
						stats.add(CONSENTS, consents);
						stats.add("retailers", new JsonObject());
						aggregationResult.add(STATS, stats);
					}
					searchResponseList.add(aggregationResult);
				}
			}
			JsonArray sortedResponseList = new JsonArray();
			for (JsonElement originalEl : properties) {
				JsonObject originalObj = originalEl.getAsJsonObject().get("hash").getAsJsonObject();
				String brandCode = originalObj.get(Constants.Consent.BRAND_CODE).getAsString();
				boolean isMatched = false;
				for (JsonElement subResponseEl : searchResponseList) {
					JsonObject subResponseObj = subResponseEl.getAsJsonObject();
					if (brandCode.equals(subResponseObj.get(Constants.Consent.BRAND_CODE).getAsString())) {
						sortedResponseList.add(subResponseObj);
						isMatched = true;
						break;
					}
				}
				if (!isMatched) {
					JsonObject stats = new JsonObject();
					JsonObject consents = new JsonObject();
					consents.addProperty(APPROVAL, 0);
					consents.addProperty(REJECTION, 0);
					consents.addProperty(TOTAL, 0);
					stats.add(CONSENTS, consents);
					JsonObject retailers = new JsonObject();
					retailers.addProperty(TOTAL, 0);
					stats.add("retailers", retailers);
					JsonObject subResponseObj = new JsonObject();
					subResponseObj.addProperty(Constants.Consent.BRAND_CODE, brandCode);
					subResponseObj.add(STATS, stats);
					sortedResponseList.add(subResponseObj);
				}
			}
			return sortedResponseList;
		} catch (Exception e) {
			logger.startOperation("searchBrands").fail(e);
			JsonArray sortedResponseList = new JsonArray();
			JsonObject errorObj = getErrorResponse("H103", "Sunucuda arama yapılamadı.");
			sortedResponseList.add(errorObj);
			return sortedResponseList;
		}
	}

	/**
	 * This method searches for consent documents which have field values that are given through fieldObj. Then it
	 * returns found documents as a json object. It also finds status sizes and return this information.
	 *
	 * @param index
	 * @param termField
	 * @param queryExtensions
	 * @param fieldObj
	 * @return
	 */
	public JsonObject searchConsentsBooleanWithFields(String index, String termField, JsonObject queryExtensions,
			JsonObject fieldObj) {
		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder finalQuery = getSearchConsentsQuery(index, queryExtensions, fieldObj);
		// add term aggregation
		finalQuery.aggregation(getTermAggregation(BY_STATUS, termField));
		SearchResponse searchResponse;
		try {
			searchResponse = getSearchResponse(index, searchRequest, finalQuery);
			JsonObject statsObj = new JsonObject();
			int totalSize = 0;
			JsonArray indexDocuments = new JsonArray();
			if (searchResponse != null) {
				Terms agg = searchResponse.getAggregations().get(BY_STATUS);
				for (Terms.Bucket entry : agg.getBuckets()) {
					statsObj.addProperty(entry.getKey().toString(), entry.getDocCount());
					totalSize += entry.getDocCount();
				}
				statsObj.addProperty(TOTAL_, totalSize);
				SearchHit[] searchHits = searchResponse.getHits().getHits();
				// add search hits objects to indexDocuments array
				indexDocuments.addAll(getSearchHits(searchHits));
			}
			JsonObject searchResponseObj = new JsonObject();
			searchResponseObj.add(CONSENTS, indexDocuments);
			searchResponseObj.add(STATS, statsObj);
			return searchResponseObj;
		} catch (Exception e) {
			logger.startOperation("searchConsentsBooleanWithFields").fail(e);
			return getErrorResponse("H103", "Sunucuda arama yapılamadı.");
		}

	}

	/**
	 * This method searches for consent documents which have field values that are given through fieldObj. Then it
	 * returns found documents as a json object. It also finds status sizes and return this information.
	 *
	 * @param indexName
	 * @param intField
	 * @param boolQueryMap
	 * @return
	 * @throws Exception
	 */
	public JsonObject searchConsentsBooleanWithFields(String indexName, String intField,
			Map<String, Object> boolQueryMap) throws Exception {
		SearchRequest searchRequest = new SearchRequest(indexName);
		BoolQueryBuilder finalBoolQuery = new BoolQueryBuilder();
		BoolQueryBuilder query = createBoolQueryWithMap(boolQueryMap, intField);
		finalBoolQuery.must(query);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchSourceBuilder finalQuery = searchSourceBuilder.query(finalBoolQuery).size(1);
		SearchResponse searchResponse = getSearchResponse(indexName, searchRequest, finalQuery);
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		JsonObject uniqueDoc = null;
		// always only 1 document
		for (SearchHit hit : searchHits) {
			uniqueDoc = new JsonObject();
			JsonElement hitEl = new Gson().fromJson(hit.toString(), JsonElement.class);
			JsonObject hitObj = hitEl.getAsJsonObject();
			JsonObject documentObj = hitObj.get(Constants._SOURCE).getAsJsonObject();
			String _id = hitObj.get(Constants._ID).getAsString();
			uniqueDoc.addProperty(Constants._ID, _id);
			uniqueDoc.add(Constants._DOC, documentObj);
			break;
		}
		return uniqueDoc;
	}

	/**
	 * This method searches for consent documents which have field values that are given through fieldObj. Then it
	 * returns found documents as a json object. It also finds status sizes and return this information.
	 *
	 * @param indexName
	 * @param termField
	 * @param queryExtensions
	 * @param fieldObj
	 * @return
	 */
	public JsonObject searchConsentsBooleanWithFieldsGet(String indexName, String termField, JsonObject queryExtensions,
			JsonObject fieldObj) {
		SearchRequest searchRequest = new SearchRequest(indexName);
		SearchSourceBuilder finalQuery = getSearchConsentsQuery(indexName, queryExtensions, fieldObj);
		// add term aggregation
		finalQuery.aggregation(getTermAggregation(BY_STATUS, termField));
		SearchResponse searchResponse;
		try {
			searchResponse = getSearchResponse(indexName, searchRequest, finalQuery);
			JsonObject paginationObj = new JsonObject();
			JsonObject stats = new JsonObject();
			stats.addProperty("approval", 0);
			stats.addProperty("rejection", 0);
			int totalSize = 0;
			JsonArray indexDocuments = new JsonArray();
			if (searchResponse != null) {
				Terms agg = searchResponse.getAggregations().get(BY_STATUS);
				for (Terms.Bucket entry : agg.getBuckets()) {
					// statsObj.addProperty(entry.getKey().toString(), entry.getDocCount());
					totalSize += entry.getDocCount();
					if (entry.getKeyAsString().equals("ONAY")) {
						stats.addProperty("approval", entry.getDocCount());
					} else if (entry.getKeyAsString().equals("RET")) {
						stats.addProperty("rejection", entry.getDocCount());
					}
				}
				SearchHit[] searchHits = searchResponse.getHits().getHits();
				// add search hits objects to indexDocuments array
				indexDocuments.addAll(getSearchHits(searchHits));
			}
			paginationObj.addProperty("totalCount", totalSize);
			paginationObj.addProperty("offset", queryExtensions.get("after").getAsNumber());
			paginationObj.addProperty("pageSize", indexDocuments.size());
			JsonObject searchResponseObj = new JsonObject();
			searchResponseObj.add("list", indexDocuments);
			searchResponseObj.add("pagination", paginationObj);
			searchResponseObj.add("stats", stats);
			return searchResponseObj;
		} catch (Exception e) {
			JsonObject errorObj = getErrorResponse("H103", "Sunucuda arama yapılamadı.");
			logger.startOperation("searchConsentsBooleanWithFieldsGet").fail(e);
			return errorObj;
		}
	}

	/**
	 * This method searches for documents which have field values that are given through fieldObj. Then it returns found
	 * documents as a json array.
	 *
	 * @param index
	 * @param queryExtentions
	 * @param jsonObj
	 * @return
	 * @throws Exception
	 */
	public JsonArray searchDailyStatusChanges(String index, JsonObject queryExtentions, JsonObject jsonObj)
			throws Exception {
		Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(index);
		searchRequest.scroll(scroll);

		DateHistogramAggregationBuilder createDateHistogramAggregation = createDateHistogramAggregation(
				queryExtentions.get(QUERY_FIELD).getAsString());

		SearchSourceBuilder finalQuery = getDailyStatusChangesQuery(index, jsonObj,
				queryExtentions.get(MAIN_FIELD).getAsString(), createDateHistogramAggregation);

		SearchResponse searchResponse = getSearchResponse(index, searchRequest, finalQuery);

		JsonArray statusReportMap = new JsonArray();
		JsonObject statusChangeObj = new JsonObject();
		if (searchResponse != null) {
			Terms statusAggregation = searchResponse.getAggregations().get(BY_STATUS);
			for (Terms.Bucket bucket : statusAggregation.getBuckets()) {
				String statusType = bucket.getKey().toString();

				Map<String, org.elasticsearch.search.aggregations.Aggregation> dateHistogramAggs = bucket
						.getAggregations().getAsMap();
				for (String key : dateHistogramAggs.keySet()) {
					ParsedDateHistogram dateHistogram = (ParsedDateHistogram) dateHistogramAggs.get(key);
					for (Bucket entry : dateHistogram.getBuckets()) {
						String date = entry.getKey().toString();
						date = date.replace("T", " ").replace("Z", "").replace("'", "");
						date = date + ":00";
						long count = entry.getDocCount();
						if (!statusChangeObj.has(date)) {
							JsonObject stats = new JsonObject();
							stats.addProperty(TOTAL_, 0);
							statusChangeObj.add(date, stats);
						}
						if (count > 0) {
							if (statusChangeObj.get(date).getAsJsonObject().has(statusType)) {
								statusChangeObj.get(date).getAsJsonObject().addProperty(statusType,
										statusChangeObj.get(date).getAsJsonObject().get(statusType).getAsInt() + count);
							} else {
								statusChangeObj.get(date).getAsJsonObject().addProperty(statusType, count);
							}
							statusChangeObj.get(date).getAsJsonObject().addProperty(TOTAL_,
									statusChangeObj.get(date).getAsJsonObject().get(TOTAL_).getAsInt() + count);
						}

					}
				}
			}

			for (String dateKey : statusChangeObj.keySet()) {
				JsonObject reportObj = new JsonObject();
				reportObj.addProperty(DATE, dateKey);
				reportObj.add(STATS, statusChangeObj.get(dateKey).getAsJsonObject());
				statusReportMap.add(reportObj);
			}
		}
		return statusReportMap;
	}

	/**
	 * This method searches for retailer codes which related to given fields in fieldObj object. If the fieldObj is
	 * null, then it searches for all retailerCodes.
	 *
	 * @param index    elastic search index which retailers index in.
	 * @param fieldObj json object which is service request.
	 * @return retailers with retailer access count.
	 * @throws Exception
	 */
	public Map<String, Long> searchRetailersWithDocCount(String index, JsonObject fieldObj) throws Exception {
		OperationLog log = logger.startOperation("searchRetailersWithDocCount");
		Map<String, Long> map = new HashMap<>();
		SearchResponse searchResponse = getSearchResponse(index, fieldObj, log);
		if (searchResponse != null) {
			Nested nestedAgg = searchResponse.getAggregations().get(BYRETAILERS);
			Terms name = nestedAgg.getAggregations().get(Constants.Consent.RETAILER_CODE);
			if (name != null) {
				for (Terms.Bucket entry : name.getBuckets()) {
					map.put(entry.getKey().toString(), entry.getDocCount());
				}
			}
		}
		return map;
	}

	public int update(String indexName, String data, String id) throws IOException {
		return updateData(new UpdateRequest(indexName, id).doc(data, XContentType.JSON));
	}

	/**
	 * This method saves given data to given index in elasticsearch.
	 *
	 * @param request
	 * @throws IOException
	 */
	public int updateData(UpdateRequest request) throws IOException {
		try {
			return getClient().update(request, RequestOptions.DEFAULT).status().getStatus();
		} catch (Throwable e) {
			logger.startOperation("updateData").fail(e);
			if (isConnectionError(e)) {
				restartClient(e);
				int status = getClient().update(request, RequestOptions.DEFAULT).status().getStatus();
				logger.startOperation("overrideRestClient").addField("method", "updateData").succeed();
				return status;
			} else {
				logger.startOperation("updateData").fail(e);
				throw e;
			}
		}
	}

	public long updateRetailersInRetailerAgent(String indexName, JsonObject fieldObj, List<String> idStringMatchList,
			List<String> countList, Map<String, Object> propertyMap, boolean hasRetailerAccess) throws Exception {
		return updateRetailersInRetailerAgentOperation(indexName, fieldObj, idStringMatchList, countList, propertyMap,
				hasRetailerAccess);
	}

	public long updateRetailersInRetailerAgentOperation(String indexName, JsonObject fieldObj,
			List<String> idStringMatchList, List<String> countList, Map<String, Object> propertyMap,
			boolean hasRetailerAccess) throws Exception {
		String idScriptString = String.join(" || ", idStringMatchList);
		StringBuilder subString = new StringBuilder();
		for (String keyName : countList) {
			subString.append(" if (!ctx._source.retailerAccess.contains(params.").append(keyName)
					.append(")) {ctx._source.retailerAccess.add(params.").append(keyName).append(")} ");
		}
		String retailerAccessOverrideSubStr = "";
		if (hasRetailerAccess) {
			retailerAccessOverrideSubStr = "ctx._source.retailerAccess.removeIf(retailer -> true); ";
		}
		String retailerCodeInsertStr = "";
		if (propertyMap.containsKey("retailerCode")) {
			retailerCodeInsertStr = "ctx._source.putAll(params.retailerCode);";
		}
		String scriptBody = "if (" + idScriptString + ") { " + retailerAccessOverrideSubStr + subString
				+ retailerCodeInsertStr
				+ " ctx._source.putAll(params.lastModification); ctx._source.putAll(params.createdBy);}";
		Script script = new Script(ScriptType.INLINE, "painless", scriptBody, propertyMap);
		BoolQueryBuilder query = createBoolQuery(fieldObj);
		UpdateByQueryRequest request = new UpdateByQueryRequest(indexName).setQuery(query);
		request.setScript(script);
		return update("updateRetailersInRetailerAgent", request);
	}

	public JsonObject getDailyBrandChangesSummary(String date, String recipientType, String brandCode)
			throws Exception {
		OperationLog log = logger.startOperation("getDailyBrandChangesSummary");
		try {

			SearchRequest searchRequest = new SearchRequest("daily-history-*");

			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery()
					.must(QueryBuilders.rangeQuery("creationDate").gte(date + " 00:00:00").lte(date + " 23:59:59"))
					.must(QueryBuilders.termQuery("recipientType", recipientType))
					.must(QueryBuilders.termQuery("brandCode", brandCode)).must(QueryBuilders.existsQuery("source"))
					.mustNot(QueryBuilders.termQuery("source", "")));

			SearchSourceBuilder ssBuilder = new SearchSourceBuilder();
			ssBuilder.size(0);
			ssBuilder.query(boolQuery);

			Script script = new Script(ScriptType.INLINE, "painless",
					"doc['source'].value.indexOf('_')>0?doc['source'].value.substring(0,doc['source'].value.indexOf('_')):doc['source'].value",
					new HashMap<>());

			TermsAggregationBuilder aggregations = AggregationBuilders.terms("consent_types").field("type")
					.subAggregation(AggregationBuilders.terms("sources").script(script)
							.subAggregation(AggregationBuilders.terms("statuses").field("status")));

			ssBuilder.aggregation(aggregations);

			searchRequest.source(ssBuilder);

			SearchResponse searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);

			JsonObject dailyChangesObj = new JsonObject();

			Terms consentTypesAgg = null;
			if (searchResponse.getAggregations() != null) {
				consentTypesAgg = searchResponse.getAggregations().get("consent_types");

				JsonArray changeStats = new JsonArray();
				dailyChangesObj.add("changeStats", changeStats);

				for (Terms.Bucket bucket : consentTypesAgg.getBuckets()) {
					String consentType = (String) bucket.getKey();
					JsonObject changeStatEl = new JsonObject();
					changeStatEl.addProperty("type", consentType);
					changeStats.add(changeStatEl);
					JsonArray consentsArr = new JsonArray();
					changeStatEl.add("consents", consentsArr);

					Terms sourcesAgg = bucket.getAggregations().get("sources");
					for (Terms.Bucket sourceBucket : sourcesAgg.getBuckets()) {
						JsonObject consentEl = new JsonObject();
						String consentSource = (String) sourceBucket.getKey();
						consentEl.addProperty("source", consentSource);
						consentEl.addProperty("total", sourceBucket.getDocCount());
						Terms statusesAgg = sourceBucket.getAggregations().get("statuses");
						for (Terms.Bucket statusBucket : statusesAgg.getBuckets()) {
							String consentStatus = (String) statusBucket.getKey();
							switch (consentStatus) {
							case "ONAY":
								consentStatus = "approval";
								break;
							case "RET":
								consentStatus = "rejection";
								break;
							case "BEKLİYOR":
								consentStatus = "pending";
								break;
							default:
								consentStatus = "";
							}
							if (StringUtils.isEmpty(consentStatus))
								continue;
							consentEl.addProperty(consentStatus, statusBucket.getDocCount());
						}
						consentsArr.add(consentEl);
					}
				}

			}
			log.succeed();
			return dailyChangesObj;

		} catch (Exception e) {
			log.fail(e);
			throw e;
		}

	}

	public JsonObject getDailyDistinctBrands(String date) throws Exception {
		SearchRequest searchRequest = new SearchRequest("daily-history-*");

		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery()
				.must(QueryBuilders.rangeQuery("creationDate").gte(date + " 00:00:00").lte(date + " 23:59:59")));

		SearchSourceBuilder ssBuilder = new SearchSourceBuilder();
		ssBuilder.size(0);
		ssBuilder.query(boolQuery);

		TermsAggregationBuilder aggregations = AggregationBuilders.terms("brands").field("brandCode").size(100);

		ssBuilder.aggregation(aggregations);

		searchRequest.source(ssBuilder);

		SearchResponse searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);

		JsonObject dailyChangedBrands = new JsonObject();
		JsonArray list = new JsonArray();
		dailyChangedBrands.add("list", list);

		Terms consentTypesAgg = searchResponse.getAggregations().get("brands");

		for (Terms.Bucket bucket : consentTypesAgg.getBuckets()) {
			String hashedBrandCode = (String) bucket.getKey();
			JsonObject dummy = new JsonObject();
			dummy.addProperty("brandCode", hashedBrandCode);
			dummy.addProperty("agreementDate", date);
			dummy.addProperty("requestId", date + "_" + hashedBrandCode);
			list.add(dummy);
		}
		return dailyChangedBrands;

	}

	public boolean clearScroll(String scrollId) {
		OperationLog log = logger.startOperation("ClearScroll").addField("scrollId", scrollId, Level.DEBUG);
		ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
		clearScrollRequest.addScrollId(scrollId);
		ClearScrollResponse clearScrollResponse;
		boolean succeeded = false;
		try {
			try {
				clearScrollResponse = getClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
			} catch (Throwable e) {
				logger.startOperation("clearScroll").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					clearScrollResponse = getClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
					logger.startOperation("overrideRestClient").addField("method", "clearScroll")
							.addField("scrollId", scrollId, Level.DEBUG).succeed();
				} else {
					logger.startOperation("clearScroll").fail(e);
					throw e;
				}
			}
			succeeded = clearScrollResponse.isSucceeded();
			log.succeed();
		} catch (Exception e) {
			log.addField("error", "could not clear scroll").fail(e);
		}
		return succeeded;
	}

	private BoolQueryBuilder createBoolQuery(JsonObject fieldObj) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		for (Entry<String, JsonElement> key : fieldObj.entrySet()) {
			if (key.getKey().equals(Constants.Consent.IYS_CODE) || key.getKey().equals(Constants.Consent.BRAND_CODE)
					|| key.getKey().equals(Constants.Consent.RECIPIENT)
					|| key.getKey().equals(Constants.Consent.RECIPIENT_TYPE)
					|| key.getKey().equals(Constants.Consent.TYPE)) {
				query.must(QueryBuilders.matchQuery(key.getKey(), key.getValue().getAsString()).operator(Operator.AND));
			}
		}
		return query;
	}

	/**
	 * This method creates a MUST bool query.
	 *
	 * @param boolQueryMap
	 * @return
	 */
	private BoolQueryBuilder createBoolQueryWithMap(Map<String, Object> boolQueryMap, String intField) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		for (String key : boolQueryMap.keySet()) {
			if (key.equals(intField)) {
				query.must(QueryBuilders.matchQuery(key, (int) boolQueryMap.get(key)));
			} else {
				query.must(QueryBuilders.matchQuery(key, (String) boolQueryMap.get(key)));
			}
		}
		return query;
	}

	/**
	 * This method creates a MUST bool query.
	 *
	 * @param fieldObj
	 * @return
	 */
	private BoolQueryBuilder createBoolQueryWithObj(String indexName, JsonObject fieldObj) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		for (Entry<String, JsonElement> key : fieldObj.entrySet()) {
			if (!key.getKey().equals(SIZE) && !key.getKey().equals(AFTER)
					&& !key.getKey().equals(Constants.Pagination.OFFSET)
					&& !key.getKey().equals(Constants.Pagination.LIMIT) && !key.getKey().equals("all")
					&& !key.getKey().equals("short") && !key.getKey().equals(Constants.Consent.RETAILER_CODE)) {
				if (indexName.equals(Constants.ElasticUtils.recipientIndex)
						&& key.getKey().equals(Constants.Consent.BRAND_CODE)) {
					query.must(
							QueryBuilders.matchQuery(key.getKey(), key.getValue().getAsLong()).operator(Operator.AND));
				} else if (key.getKey().equals(Constants.Consent.BRAND_NAME)) {
					query.must(QueryBuilders.prefixQuery(key.getKey(), key.getValue().getAsString()));
				} else {
					query.must(QueryBuilders.matchQuery(key.getKey(), key.getValue().getAsString())
							.operator(Operator.AND));
				}
			}
		}
		return query;
	}

	private SearchSourceBuilder createBoolQueryForLastConsents(String indexName, JsonObject fieldObj) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().filter(createBoolQueryWithObj(indexName, fieldObj));

		SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
		searchBuilder.query(boolQuery);
		searchBuilder.sort(SortBuilders.fieldSort(Constants.Audit.CREATION_DATE).order(SortOrder.DESC));

		return searchBuilder;
	}

	/**
	 * This method creates indices of given map.
	 *
	 * @param indexInfo
	 */
	private void createIndices(Map<String, String> indexInfo, int shardCount) {
		// get index names and mappings
		for (String indexName : indexInfo.keySet()) {
			// pull index mapping of index
			try {
				String indexMapping = indexInfo.get(indexName);
				// create each index
				createIndex(indexName, indexMapping, shardCount);
			} catch (Exception e) {
				logger.error("could not create index: \"%s\"", indexName);
			}

		}
	}

	/**
	 * This method puts documents to given documents list and returns search hits obj.
	 *
	 * @param response  Response to retrieve the documents from.
	 * @param documents Array to fill with the retrieved documents.
	 * @return SearchHit array within the given {@code response}.
	 */
	private SearchHit[] getDocuments(SearchResponse response, JsonArray documents) {
		SearchHit[] searchHits = response.getHits().getHits();
		for (SearchHit hit : searchHits) {
			JsonObject documentObj = new Gson().fromJson(hit.toString(), JsonObject.class).get(Constants._SOURCE)
					.getAsJsonObject();
			documents.add(documentObj);
		}
		return searchHits;
	}

	private JsonArray getSearchHits(SearchHit[] searchHits) {
		JsonArray documents = new JsonArray();
		if (searchHits != null) {
			for (SearchHit hit : searchHits) {
				JsonElement hitEl = new Gson().fromJson(hit.toString(), JsonElement.class);
				JsonObject hitObj = hitEl.getAsJsonObject();
				JsonObject documentObj = hitObj.get(Constants._SOURCE).getAsJsonObject();
				if (documentObj.has(Constants.Consent.CONSENT_DATE)
						&& documentObj.get(Constants.Consent.CONSENT_DATE).isJsonNull()) {
					documentObj.remove(Constants.Consent.CONSENT_DATE);
				}
				if (documentObj.has(Constants.Consent.SOURCE)
						&& documentObj.get(Constants.Consent.SOURCE).isJsonNull()) {
					documentObj.remove(Constants.Consent.SOURCE);
				}
				if (documentObj.has(Constants.Consent.RETAILER_CODE)
						&& documentObj.get(Constants.Consent.RETAILER_CODE).isJsonNull()) {
					documentObj.remove(Constants.Consent.RETAILER_CODE);
				}
				documents.add(documentObj);
			}
		}
		return documents;
	}

	private SearchResponse getSearchResponse(String index, JsonObject fieldObj, OperationLog log) throws Exception {
		Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(index);
		searchRequest.scroll(scroll);

		BoolQueryBuilder query;
		if (fieldObj != null && !fieldObj.isJsonNull()) {
			query = createBoolQueryWithObj(index, fieldObj);
		} else {
			query = QueryBuilders.boolQuery();
			query.must(QueryBuilders.matchAllQuery());
		}
		TermsAggregationBuilder termsRetailers = AggregationBuilders.terms(Constants.Consent.RETAILER_CODE).size(10000)
				.field(RETAILER_ACCESS_RETAIL_CODE);
		termsRetailers
				.subAggregation(AggregationBuilders.count("retailerAccessCount").field(RETAILER_ACCESS_RETAIL_CODE));
		AggregationBuilder aggregation = AggregationBuilders.nested(BYRETAILERS, "retailerAccess")
				.subAggregation(termsRetailers);

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchSourceBuilder finalQuery = searchSourceBuilder.query(query);

		finalQuery.aggregation(aggregation);
		log.addField("finalSearchQuery", finalQuery.toString(), Level.DEBUG);
		return getSearchResponse(index, searchRequest, finalQuery);
	}

	/**
	 * This method gets search response.
	 *
	 * @param indexName
	 * @param searchRequest
	 * @param searchSourceBuilder
	 * @return
	 * @throws Exception
	 */
	private SearchResponse getSearchResponse(String indexName, SearchRequest searchRequest,
			SearchSourceBuilder searchSourceBuilder) throws Exception {
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse;
		try {
			searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
		} catch (Throwable e) {
			logger.startOperation("getSearchResponse").fail(e);
			if (isConnectionError(e)) {
				restartClient(e);
				searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
				logger.startOperation("overrideRestClient").addField("method", "getSearchResponse")
						.addField("indexName", indexName, Level.DEBUG)
						.addField("searchRequest", searchRequest.toString(), Level.DEBUG).succeed();
			} else {
				logger.startOperation("getFromIndex").fail(e);
				throw e;
			}
		}
		return searchResponse;
	}

	private void restartClient(Throwable cause) {
		if (!this.restarting.get()) {
			synchronized (this.restarting) {
				if (!this.restarting.get()) {
					OperationLog log = logger.startOperation("restartClient");
					if (cause != null) {
						log.addField("cause", ExceptionUtils.getStackTrace(cause));
					}
					try {
						this.restarting.set(true);
						this.client.close();
						this.client = null;
						getClient();
						this.restarting.set(false);
					} catch (Exception e) {
						log.fail(e);
					}
					log.succeed();
				}
			}
		}
	}

	/**
	 * This method scrolls documents and stores all of them in indexDocuments array and then returns this array.
	 *
	 * @param scroll
	 * @param searchResponse
	 * @return
	 */
	public Map<String, SearchHit[]> scrollDocumentsToMigrate(final Scroll scroll, SearchResponse searchResponse) {
		// BulkRequest bulkRequest = new BulkRequest();
		Map<String, SearchHit[]> hitsMap = new HashMap<String, SearchHit[]>();
		if (searchResponse != null) {
			String scrollId = searchResponse.getScrollId();

			SearchHits hits = searchResponse.getHits();
			SearchHit[] searchHits = hits.getHits();

			// add search hits objects to indexDocuments array
			if (searchHits != null) {
				hitsMap.put(scrollId, searchHits);

				// for (SearchHit hit : searchHits) {
				// JsonElement hitEl = new Gson().fromJson(hit.toString(), JsonElement.class);
				// JsonObject hitObj = hitEl.getAsJsonObject();
				// JsonObject documentObj = hitObj.get(Constants._SOURCE).getAsJsonObject();
				// JsonArray actionHistoryArr = documentObj.get(Constants.ActionHistory.ACTION_HISTORY)
				// .getAsJsonArray();
				// for (JsonElement actionLogEl : actionHistoryArr) {
				// JsonObject actionLog = actionLogEl.getAsJsonObject();
				//
				// }
				// }
			}

			// while (searchHits != null && searchHits.length > 0) {
			// if (scroll != null) {
			// SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
			// scrollRequest.scroll(scroll);
			// try {
			// try {
			// searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
			// } catch (Throwable e) {
			// logger.startOperation("scrollDocuments").fail(e);
			// if (isConnectionError(e)) {
			// restartClient(e);
			// searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
			// logger.startOperation("overrideRestClient").addField("method", "scrollDocuments")
			// .succeed();
			// } else {
			// logger.startOperation("scrollDocuments").fail(e);
			// throw e;
			// }
			// }
			// } catch (Exception e) {
			// logger.error(String.format("could not scroll! scroll id: \"%s\"", scrollId), e);
			// }
			// scrollId = searchResponse.getScrollId();
			// searchHits = searchResponse.getHits().getHits();
			// if (searchHits != null) {
			// for (SearchHit hit : searchHits) {
			// JsonElement hitEl = new Gson().fromJson(hit.toString(), JsonElement.class);
			// JsonObject hitObj = hitEl.getAsJsonObject();
			// JsonObject documentObj = hitObj.get(Constants._SOURCE).getAsJsonObject();
			//
			// }
			// }
			// } else {
			// break;
			// }
			// }
			// if (scrollId != null) {
			// boolean succeeded = clearScroll(scrollId);
			// logger.debug(String.format("clear scroll status: \"%s\"", succeeded));
			// }
		}
		return hitsMap;
	}

	public Map<String, SearchHit[]> getRemaining(Scroll scroll, String scrollId, SearchScrollRequest scrollRequest) {
		Map<String, SearchHit[]> hitsMap = new HashMap<String, SearchHit[]>();
		try {
			try {
				SearchResponse searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
				String scrollIdNew = searchResponse.getScrollId();
				if (scrollIdNew != null) {
					SearchHit[] hits = searchResponse.getHits().getHits();
					if (hits.length > 0) {
						hitsMap.put(scrollIdNew, hits);
					}
				}
			} catch (Throwable e) {
				logger.startOperation("scrollDocuments").fail(e);
				if (isConnectionError(e)) {
					restartClient(e);
					SearchResponse searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
					String scrollIdNew = searchResponse.getScrollId();
					if (scrollIdNew != null) {

						SearchHit[] hits = searchResponse.getHits().getHits();
						if (hits.length > 0) {
							hitsMap.put(scrollIdNew, hits);
						}
					}
					logger.startOperation("overrideRestClient").addField("method", "scrollDocuments").succeed();
				} else {
					logger.startOperation("scrollDocuments").fail(e);
					throw e;
				}
			}
		} catch (Exception e) {
			logger.error(String.format("could not scroll! scroll id: \"%s\"", scrollId), e);
		}

		return hitsMap;
	}

	/**
	 * This method scrolls documents and stores all of them in indexDocuments array and then returns this array.
	 *
	 * @param scroll
	 * @param searchResponse
	 * @return
	 */
	private JsonArray scrollDocuments(final Scroll scroll, SearchResponse searchResponse) {
		JsonArray indexDocuments = new JsonArray();
		if (searchResponse != null) {
			String scrollId = searchResponse.getScrollId();
			SearchHit[] searchHits = searchResponse.getHits().getHits();

			// add search hits objects to indexDocuments array
			indexDocuments.addAll(getSearchHits(searchHits));

			while (searchHits != null && searchHits.length > 0) {
				if (scroll != null) {
					SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
					scrollRequest.scroll(scroll);
					try {
						try {
							searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
						} catch (Throwable e) {
							logger.startOperation("scrollDocuments").fail(e);
							if (isConnectionError(e)) {
								restartClient(e);
								searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
								logger.startOperation("overrideRestClient").addField("method", "scrollDocuments")
										.succeed();
							} else {
								logger.startOperation("scrollDocuments").fail(e);
								throw e;
							}
						}
					} catch (Exception e) {
						logger.error(String.format("could not scroll! scroll id: \"%s\"", scrollId), e);
					}
					scrollId = searchResponse.getScrollId();
					searchHits = searchResponse.getHits().getHits();
					indexDocuments.addAll(getSearchHits(searchHits));
				} else {
					break;
				}
			}
			if (scrollId != null) {
				boolean succeeded = clearScroll(scrollId);
				logger.debug(String.format("clear scroll status: \"%s\"", succeeded));
			}
		}
		return indexDocuments;
	}

	private boolean isConnectionError(Throwable e) {
		return e.getMessage().contains("Connection reset by peer")
				|| e.getMessage().contains("Request cannot be executed; I/O reactor status: STOPPED");
	}

	private SearchHit[] searchById(String index, String id) throws Exception {
		SearchResponse searchResponse = getSearchResponse(index, new SearchRequest(index),
				new SearchSourceBuilder().query(QueryBuilders.matchQuery("_id", id)).size(1));
		return searchResponse.getHits().getHits();
	}

	private long update(String method, UpdateByQueryRequest request) throws Exception {
		OperationLog log = logger.startOperation(method);
		try {
			try {
				BulkByScrollResponse resp = getClient().updateByQuery(request, RequestOptions.DEFAULT);
				log.succeed();
				return resp.getUpdated();
			} catch (Throwable e) {
				logger.error("unable to update script query", e);
				if (isConnectionError(e)) {
					restartClient(e);
					BulkByScrollResponse resp = getClient().updateByQuery(request, RequestOptions.DEFAULT);
					logger.startOperation(method).addField("method", method).succeed();
					log.succeed();
					return resp.getUpdated();
				} else {
					logger.error("unable to update script query", e);
					throw e;
				}
			}
		} catch (Exception e) {
			log.fail(e);
			throw e;
		}
	}

	public JsonObject searchDetailConsentChanges(String after, Integer limit, String source, String hashedBrandCode)
			throws Exception {
		OperationLog log = logger.startOperation("searchDetailConsentChanges");
		try {

			if (StringUtils.isNotEmpty(after) && after.indexOf("_") < 1) {
				throw new Exception("error");
			}

			SearchRequest searchRequest = new SearchRequest("daily-history-*");
			String sourceType = Optional.ofNullable(source).orElse("IYS");
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			LocalDateTime now = LocalDateTime.now();
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
					.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("brandCode", hashedBrandCode))
							.must(QueryBuilders.wildcardQuery("source", sourceType + "*"))
							.must(QueryBuilders.rangeQuery("creationDate").gte(now.minusHours(72).format(formatter))
									.lte(now.minusHours(1).format(formatter))));

			SearchSourceBuilder ssBuilder = new SearchSourceBuilder();
			ssBuilder.size(Optional.ofNullable(limit).filter(value -> (value > 0 && value < 1000)).orElse(1000));
			ssBuilder.query(boolQuery);
			ssBuilder.sort(SortBuilders.fieldSort("creationDate").order(SortOrder.ASC));
			ssBuilder.sort(SortBuilders.fieldSort("transactionId").order(SortOrder.ASC));

			if (StringUtils.isNotEmpty(after)) {
				String[] afterValues = after.split("_");
				ssBuilder.searchAfter(new Object[] { Long.valueOf(afterValues[0]), afterValues[1] });
			}

			searchRequest.source(ssBuilder);

			SearchResponse searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
			int len = searchResponse.getHits().getHits().length;
			JsonObject dailyChangesObj = new JsonObject();

			if (searchResponse != null && len > 0) {
				Object[] sortValues = searchResponse.getHits().getHits()[len - 1].getSortValues();
				dailyChangesObj.addProperty("after", sortValues[0] + "_" + sortValues[1]);
				JsonArray list = new JsonArray();
				for (SearchHit sh : searchResponse.getHits().getHits()) {
					Map<String, Object> sourceAsMap = sh.getSourceAsMap();
					JsonObject temp = new JsonObject();
					if (sourceAsMap.containsKey("consentDate")) {
						temp.addProperty("consentDate", sourceAsMap.get("consentDate").toString());
					}
					if (sourceAsMap.containsKey("creationDate")) {
						temp.addProperty("creationDate", sourceAsMap.get("creationDate").toString());
					}
					if (sourceAsMap.containsKey("source")) {
						temp.addProperty("source", sourceAsMap.get("source").toString());
					}
					if (sourceAsMap.containsKey("recipient")) {
						temp.addProperty("recipient", sourceAsMap.get("recipient").toString());
					}
					if (sourceAsMap.containsKey("recipientType")) {
						temp.addProperty("recipientType", sourceAsMap.get("recipientType").toString());
					}
					if (sourceAsMap.containsKey("status")) {
						temp.addProperty("status", sourceAsMap.get("status").toString());
					}
					if (sourceAsMap.containsKey("type")) {
						temp.addProperty("type", sourceAsMap.get("type").toString());
					}
					if (sourceAsMap.containsKey("transactionId")) {
						temp.addProperty("transactionId", sourceAsMap.get("transactionId").toString());
					}
					list.add(temp);
				}
				dailyChangesObj.add("list", list);
			}
			log.succeed();
			return dailyChangesObj;
		}

		catch (Exception e) {
			log.fail(e);
			throw e;

		}
	}

	public void createDailyChangesFile(String hashedBrandCode, String date, String fileURI, Integer chunkSize) throws Exception {
		FileWriter csvWriter = null;
		try {
			OperationLog log = logger.startOperation("createDailyChangesFile");

			final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
			SearchRequest searchRequest = new SearchRequest("daily-history-*");
			searchRequest.scroll(scroll);
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
					.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("brandCode", hashedBrandCode)).must(
							QueryBuilders.rangeQuery("creationDate").gte(date + " 00:00:00").lte(date + " 23:59:59")));
			
			SearchSourceBuilder ssBuilder = new SearchSourceBuilder();
			ssBuilder.query(boolQuery);
			ssBuilder.size(chunkSize);
			ssBuilder.sort(SortBuilders.fieldSort("creationDate").order(SortOrder.ASC));
			ssBuilder.sort(SortBuilders.fieldSort("transactionId").order(SortOrder.ASC));

			searchRequest.source(ssBuilder);

			SearchResponse searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);

			csvWriter = new FileWriter(fileURI);

			if (searchResponse != null) {
				String scrollId = searchResponse.getScrollId();
				if(searchResponse!=null && searchResponse.getTook() != null)
					logger.warn("hashed brand code: "+hashedBrandCode+" change file date: "+date+" elapsed scroll time  (milli seconds): "+ searchResponse.getTook().getMillis());
				SearchHit[] searchHits = searchResponse.getHits().getHits();

				// add search hits objects to indexDocuments array

				while (searchHits != null && searchHits.length > 0) {
					for (SearchHit sh : searchResponse.getHits().getHits()) {
						Map<String, Object> sourceAsMap = sh.getSourceAsMap();
						List<String> line = new ArrayList<>();

						// creationDate,transaxtionId,recipient,recipientType,type,status,consentDate,source
						if (sourceAsMap.containsKey("creationDate")) {
							line.add(sourceAsMap.get("creationDate").toString());
						} else {
							line.add("");
						}
						if (sourceAsMap.containsKey("transactionId")) {
							line.add(sourceAsMap.get("transactionId").toString());
						} else {
							line.add("");
						}
						if (sourceAsMap.containsKey("recipient")) {
							line.add(sourceAsMap.get("recipient").toString());
						} else {
							line.add("");
						}
						if (sourceAsMap.containsKey("recipientType")) {
							line.add(sourceAsMap.get("recipientType").toString());
						} else {
							line.add("");
						}
						if (sourceAsMap.containsKey("type")) {
							line.add(sourceAsMap.get("type").toString());
						} else {
							line.add("");
						}
						if (sourceAsMap.containsKey("status")) {
							line.add(sourceAsMap.get("status").toString());
						} else {
							line.add("");
						}
						if (sourceAsMap.containsKey("consentDate")) {
							line.add(sourceAsMap.get("consentDate").toString());
						} else {
							line.add("");
						}
						if (sourceAsMap.containsKey("source")) {
							line.add(sourceAsMap.get("source").toString());
						} else {
							line.add("");
						}
						csvWriter.append(String.join(",", line));
						csvWriter.append("\n");

					}
					if (scroll != null) {
						SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
						scrollRequest.scroll(scroll);
						try {
							try {
								searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
							} catch (Throwable e) {
								logger.startOperation("scrollDocuments").fail(e);
								if (isConnectionError(e)) {
									restartClient(e);
									searchResponse = getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
									logger.startOperation("overrideRestClient").addField("method", "scrollDocuments")
											.succeed();
								} else {
									logger.startOperation("scrollDocuments").fail(e);
									throw e;
								}
							}
						} catch (Exception e) {
							logger.error(String.format("could not scroll! scroll id: \"%s\"", scrollId), e);
						}
						scrollId = searchResponse.getScrollId();
						searchHits = searchResponse.getHits().getHits();
						if(searchResponse!=null && searchResponse.getTook() != null)
							logger.warn("hashed brand code: "+hashedBrandCode+" change file date: "+date+" elapsed scroll time (milli seconds): "+ searchResponse.getTook().getMillis());
					} else {
						break;
					}
				}
				if (scrollId != null) {
					boolean succeeded = clearScroll(scrollId);
					logger.debug(String.format("clear scroll status: \"%s\"", succeeded));
				}
			}

		}

		catch (Exception e) {
			throw e;
		} finally {
			if (csvWriter != null) {
				csvWriter.flush();
				csvWriter.close();
			}
		}
	}

	public JsonObject getAgreementStatus(String date, String brandCode, boolean getDetails) throws Exception {
		OperationLog log = logger.startOperation("getAgreementStatus");

		try {
			SearchRequest searchRequest = new SearchRequest("changesmetadata");
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
					.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("agreementDate", date))
							.must(QueryBuilders.termQuery("brandCode", brandCode)));
			SearchSourceBuilder ssBuilder = new SearchSourceBuilder();
			ssBuilder.size(1);
			ssBuilder.query(boolQuery);
			searchRequest.source(ssBuilder);
			SearchResponse searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);

			JsonObject agreementStatus = new JsonObject();

			if (searchResponse != null && searchResponse.getHits().getHits().length > 0) {
				for (SearchHit sh : searchResponse.getHits().getHits()) {
					Map<String, Object> sourceAsMap = sh.getSourceAsMap();
					if (sourceAsMap.containsKey("changesFileStatus")) {
						agreementStatus.addProperty("processStatus", sourceAsMap.get("changesFileStatus").toString());
					}
					if (sourceAsMap.containsKey("changesFileHashValue")) {
						agreementStatus.addProperty("hashValue", sourceAsMap.get("changesFileHashValue").toString());
					}
					if (sourceAsMap.containsKey("changesFileHashMethod")) {
						agreementStatus.addProperty("hashMethod", sourceAsMap.get("changesFileHashMethod").toString());
					}
					if (sourceAsMap.containsKey("agreementStatus")) {
						agreementStatus.addProperty("agreementStatus", sourceAsMap.get("agreementStatus").toString());
					}
					if (getDetails && sourceAsMap.containsKey("changesFileURI")) {
						agreementStatus.addProperty("changesFileURI", sourceAsMap.get("changesFileURI").toString());
					}
					if (getDetails && sourceAsMap.containsKey("timeStampStatus")) {
						agreementStatus.addProperty("timeStampStatus", sourceAsMap.get("timeStampStatus").toString());
					}
					if (getDetails && sourceAsMap.containsKey("timeStampFile")) {
						agreementStatus.addProperty("timeStampFile", sourceAsMap.get("timeStampFile").toString());
					}
				}
			}
			log.succeed();
			return agreementStatus;

		} catch (Exception e) {
			log.fail(e);
			throw e;
		}

	}

	public JsonObject captureAgreement(String date, String brandCode) throws Exception {
		OperationLog log = logger.startOperation("getAgreementStatus");

		try {
			SearchRequest searchRequest = new SearchRequest("changesmetadata");
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
					.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("agreementDate", date))
							.must(QueryBuilders.termQuery("brandCode", brandCode)));
			SearchSourceBuilder ssBuilder = new SearchSourceBuilder();
			ssBuilder.size(1);
			ssBuilder.query(boolQuery);
			searchRequest.source(ssBuilder);
			SearchResponse searchResponse = getClient().search(searchRequest, RequestOptions.DEFAULT);
			JsonObject retObj = new JsonObject();

			if (searchResponse != null && searchResponse.getHits().getHits().length > 0) {
				for (SearchHit sh : searchResponse.getHits().getHits()) {
					String agreementStatusStr = sh.getSourceAsString();
					JsonObject o = JsonParser.parseString(agreementStatusStr).getAsJsonObject();
					o.addProperty("agreementStatus", true);
					Date now = Calendar.getInstance().getTime();
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
					o.addProperty("agreementCaptureDate", sdf.format(now));
					update("changesmetadata", o.toString(), sh.getId());
					retObj.addProperty("agreementCaptured", true);
				}
			}
			log.succeed();

			return retObj;
		} catch (Exception e) {
			log.fail(e);
			throw e;
		}
	}

	public BulkProcessor getBulkProcessor() {
		return bulkProcessor;
	}

}
