/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.elasticsearch;

import org.junit.Test;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.core.Bulk;
import io.confluent.connect.elasticsearch.bulk.BulkRequest;
import io.confluent.connect.elasticsearch.jest.JestBulkRequest;
import io.confluent.connect.elasticsearch.jest.JestElasticsearchClient;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BulkIndexingClientTest {

  private JestClient jestClient;
  private NodesInfo info;

  @Before
  public void setUp() throws Exception {
    jestClient = mock(JestClient.class);
    info = new NodesInfo.Builder().addCleanApiParameter("version").build();
    JsonObject nodeRoot = new JsonObject();
    nodeRoot.addProperty("version", "1.0");
    JsonObject nodesRoot = new JsonObject();
    nodesRoot.add("localhost", nodeRoot);
    JsonObject nodesInfo = new JsonObject();
    nodesInfo.add("nodes", nodesRoot);
    JestResult result = new JestResult(new Gson());
    result.setJsonObject(nodesInfo);
    when(jestClient.execute(info)).thenReturn(result);
  }

  @Test
  public void testSetParameters() throws Exception {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("pipeline", "pipeline_id_1");

    JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
    BulkIndexingClient bulkIndexingClient = new BulkIndexingClient(client, parameters);
    Key key = new Key("index", "test_type", "1");
    String payload = "{\"hello\": true}";
    IndexableRecord record = new IndexableRecord(key, payload, 1L);

    JestBulkRequest bulkRequest = (JestBulkRequest)bulkIndexingClient.bulkRequest(Collections.singletonList(record));
    Bulk bulk = bulkRequest.getBulk();
    Object[] pipeline = bulk.getParameter("pipeline").toArray();

    assertEquals(pipeline.length, 1);
    assertEquals("pipeline_id_1", pipeline[0]);
    String uri = bulk.getURI();
    assertTrue(uri.contains("pipeline=pipeline_id_1"));
  }
}
