package com.lg;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonObject;

public class ESTest {
	private static String host="192.168.40.100";
	private static int port=9300;
	private TransportClient transportClient;
	private Builder builder = Settings.builder().put("cluster-name","my-application");
	@Before
	public void getClient() throws Exception{
		transportClient=new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
		//transportClient=new PreBuiltTransportClient(builder.build()).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
	}
	
	@After
	public void closed(){
		transportClient.close();
	}
	
	@Test
	public void testIndex(){
		for (int i = 0; i < 5; i++) {
			JsonObject jsonObject=new JsonObject();
			jsonObject.addProperty("name", "java编程思想"+i);
			jsonObject.addProperty("publishDate", "2012.12.12");
			jsonObject.addProperty("price", "11"+i);
			IndexResponse response = transportClient.prepareIndex("book","java").setSource(jsonObject.toString(),XContentType.JSON).get();
			System.out.println(response.status());
			System.out.println(response.getIndex());
			System.out.println(response.getType());
			System.out.println(response.getVersion());
		}
	}
	
	@Test
	public void testGet(){
		GetResponse response = transportClient.prepareGet("book","java","1").get();
		System.out.println(response.getSourceAsString());
	}
	
	@Test
	public void testUpdate(){
		JsonObject jsonObject=new JsonObject();
		jsonObject.addProperty("name", "java编程思想2");
		jsonObject.addProperty("publishDate", "2012.12.12");
		jsonObject.addProperty("price", "112");
		UpdateResponse response = transportClient.prepareUpdate("book","java","1").setDoc(jsonObject.toString(),XContentType.JSON).get();
		System.out.println(response.status());
		System.out.println(response.getIndex());
		System.out.println(response.getType());
		System.out.println(response.getVersion());
	}
	
	@Test
	public void testDelete(){
		DeleteResponse response = transportClient.prepareDelete("book","java","AWbJcMUu7jLnt4uj7xKk").get();
		System.out.println(response.status());
		System.out.println(response.getIndex());
		System.out.println(response.getType());
		System.out.println(response.getVersion());
	}
	
	@Test
	public void queryAll(){
		SearchRequestBuilder builder = transportClient.prepareSearch("book").setTypes("java");
		SearchResponse response = builder.setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	@Test
	public void queryByPage(){
		SearchRequestBuilder builder = transportClient.prepareSearch("book").setTypes("java");
		SearchResponse response = builder.setQuery(QueryBuilders.matchAllQuery())
				.setFrom(0)
				.setSize(2)
				.execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	@Test
	public void querySort(){
		SearchRequestBuilder builder = transportClient.prepareSearch("book").setTypes("java");
		SearchResponse response = builder.setQuery(QueryBuilders.matchAllQuery())
				.addSort("name",SortOrder.DESC)
				.execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	@Test
	public void queryInclude(){
		SearchRequestBuilder builder = transportClient.prepareSearch("book").setTypes("java");
		SearchResponse response = builder.setQuery(QueryBuilders.matchAllQuery())
				.setFetchSource(new String[]{"name","price"}, null)
				.execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	@Test
	public void queryByCondition(){
		SearchRequestBuilder builder = transportClient.prepareSearch("book").setTypes("java");
		SearchResponse response = builder.setQuery(QueryBuilders.matchQuery("name", "4"))
				.setFetchSource(new String[]{"name","price"}, null)
				.execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	@Test
	public void queryHighLight(){
		SearchRequestBuilder builder = transportClient.prepareSearch("book").setTypes("java");
		HighlightBuilder highlightBuilder=new HighlightBuilder();
		highlightBuilder.preTags("<h1>");
		highlightBuilder.postTags("</h1>");
		highlightBuilder.field("name");
		SearchResponse response = builder.setQuery(QueryBuilders.matchQuery("name", "4"))
				.highlighter(highlightBuilder)
				.setFetchSource(new String[]{"name","price"}, null)
				.execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
			System.out.println(searchHit.getHighlightFields());
		}
	}
	
	@Test
	public void queryMulti(){
		SearchRequestBuilder builder = transportClient.prepareSearch("book").setTypes("java");
		QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("name", "4");
		QueryBuilder queryBuilder1=QueryBuilders.matchPhraseQuery("name", "java");
		SearchResponse response = builder.setQuery(QueryBuilders.boolQuery().must(queryBuilder).must(queryBuilder1))
				.setFetchSource(new String[]{"name","price"}, null)
				.execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	@Test
	public void queryMulti2(){
		SearchRequestBuilder builder = transportClient.prepareSearch("book").setTypes("java");
		QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("name", "4");
		QueryBuilder queryBuilder1=QueryBuilders.rangeQuery("price").gte("113");
		SearchResponse response = builder.setQuery(QueryBuilders.boolQuery().must(queryBuilder).must(queryBuilder1))
				.setFetchSource(new String[]{"name","price"}, null)
				.execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
	
	@Test
	public void queryMultiXXX(){
		SearchRequestBuilder builder = transportClient.prepareSearch("book").setTypes("java");
		SearchResponse response = builder.setQuery(QueryBuilders.multiMatchQuery("思想","name","date").analyzer("analyzer"))
				.setFetchSource(new String[]{"name","price"}, null)
				.execute().actionGet();
		SearchHits hits = response.getHits();
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSourceAsString());
		}
	}
}


