/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.adaltas.flume.serialization;

import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestJSONEventSerializer {

  File testFile = new File("src/test/resources/events.txt");
  File expectedFile = new File("src/test/resources/events.txt");

  @Test
  public void testJson() throws FileNotFoundException, IOException {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("header1", "value1");
		headers.put("header2", "value2");
		
		OutputStream out = new FileOutputStream(testFile);
		Context context = new Context();
//		context.put("body", "body");
		EventSerializer serializer =
		    EventSerializerFactory.getInstance("com.adaltas.flume.serialization.JSONEventSerializer$Builder", context, out);
		serializer.afterCreate();
		serializer.write(EventBuilder.withBody("event 1", Charsets.UTF_8, headers));
		serializer.write(EventBuilder.withBody("event 2", Charsets.UTF_8, headers));
		serializer.write(EventBuilder.withBody("event 3", Charsets.UTF_8, headers));
		serializer.flush();
		serializer.beforeClose();
		out.flush();
		out.close();
		
		BufferedReader reader = new BufferedReader(new FileReader(testFile));
		Assert.assertEquals("{\"body\":\"event 1\",\"header2\":\"value2\",\"header1\":\"value1\"}", reader.readLine());
		Assert.assertEquals("{\"body\":\"event 2\",\"header2\":\"value2\",\"header1\":\"value1\"}", reader.readLine());
		Assert.assertEquals("{\"body\":\"event 3\",\"header2\":\"value2\",\"header1\":\"value1\"}", reader.readLine());
		Assert.assertNull(reader.readLine());
		reader.close();
		
		FileUtils.forceDelete(testFile);
  }

  @Test
  public void testColumnsAndBody() throws FileNotFoundException, IOException {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("header1", "value1");
		headers.put("header2", "value2");
		headers.put("header3", "value3");
		
		OutputStream out = new FileOutputStream(testFile);
		Context context = new Context();
		context.put("columns", "header1 header3 msg");
		context.put("body", "msg");
		EventSerializer serializer =
		    EventSerializerFactory.getInstance("com.adaltas.flume.serialization.JSONEventSerializer$Builder", context, out);
		serializer.afterCreate();
		serializer.write(EventBuilder.withBody("event 1", Charsets.UTF_8, headers));
		serializer.write(EventBuilder.withBody("event 2", Charsets.UTF_8, headers));
		serializer.write(EventBuilder.withBody("event 3", Charsets.UTF_8, headers));
		serializer.flush();
		serializer.beforeClose();
		out.flush();
		out.close();
		
		BufferedReader reader = new BufferedReader(new FileReader(testFile));
		Assert.assertEquals("{\"header1\":\"value1\",\"header3\":\"value3\",\"msg\":\"event 1\"}", reader.readLine());
		Assert.assertEquals("{\"header1\":\"value1\",\"header3\":\"value3\",\"msg\":\"event 2\"}", reader.readLine());
		Assert.assertEquals("{\"header1\":\"value1\",\"header3\":\"value3\",\"msg\":\"event 3\"}", reader.readLine());
		Assert.assertNull(reader.readLine());
		reader.close();
		
		FileUtils.forceDelete(testFile);
  }

}
