package org.apache.drill.rest;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.alibaba.fastjson.JSON;

public class RestDemo {

	private static final String HOST_NAME = "http://xuansheng-pc:8047/query.json";

	private static String buildRequestBody(String queryType, String query) {
		RequestBody reques = new RequestBody(queryType, query);
		String json = JSON.toJSON(reques).toString();
		return json;
	}

	public static void main(String[] args) throws ClientProtocolException, IOException {
		String queryType = "SQL";
		String query="SELECT * FROM cp.`employee.json` LIMIT 20";
		String buildRequestBody = buildRequestBody(queryType, query);
		System.out.println("buildRequestBody:"+buildRequestBody);
		Content returnContent = Request.Post(HOST_NAME).bodyString(buildRequestBody,ContentType.APPLICATION_JSON).execute().returnContent();
		System.out.println(returnContent);
	}

}

class RequestBody {

	private String queryType;

	private String query;

	public RequestBody() {
	}

	public RequestBody(String queryType, String query) {
		super();
		this.queryType = queryType;
		this.query = query;
	}

	public String getQueryType() {
		return queryType;
	}

	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

}
