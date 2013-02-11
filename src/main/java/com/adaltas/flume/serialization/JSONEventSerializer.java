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

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * This class writes the header properties and body of the event as JSON lines. The
 * body is by default associated with the "body" key. The "columns" configuration 
 * allows to list and order the columns to write. It must contains the name of 
 * the body key if you wish to write the event body. The "body" configuration 
 * is the name of the key associated to the event body.
 * 
 */
public class JSONEventSerializer implements EventSerializer {

  private final static Logger logger =
      LoggerFactory.getLogger(JSONEventSerializer.class);

  // for legacy reasons, by default, append a newline to each event written out
  private final String APPEND_NEWLINE = "appendNewline";
  private final boolean APPEND_NEWLINE_DFLT = true;
  private final String COLUMNS = "columns";
  private final String COLUMNS_DFLT = null;
  private final String BODY = "body";
  private final String BODY_DFLT = "body";

  private final OutputStream out;
  private final boolean appendNewline;
  private final String columns;
  private final String body;

  private JSONEventSerializer(OutputStream out, Context ctx) {
    this.appendNewline = ctx.getBoolean(APPEND_NEWLINE, APPEND_NEWLINE_DFLT);
    this.columns = ctx.getString(COLUMNS, COLUMNS_DFLT);
    this.body = ctx.getString(BODY, BODY_DFLT);
    this.out = out;
  }

  @Override
  public boolean supportsReopen() {
    return true;
  }

  @Override
  public void afterCreate() {
    // noop
  }

  @Override
  public void afterReopen() {
    // noop
  }

  @Override
  public void beforeClose() {
    // noop
  }

  @Override
  public void write(Event e) throws IOException {
		Map<String,String> headers = e.getHeaders();
		Map<String,String> event;
		if(this.columns != null){
			event = new LinkedHashMap<String,String>();
			StringTokenizer tok = new StringTokenizer(this.columns);
			while(tok.hasMoreTokens()){
				String key = tok.nextToken();
				if(key.equals(this.body)){
					event.put(key, new String(e.getBody()));
				}else{
					event.put(key, headers.get(key));
				}
			}
		}else{
			// Copy since we'll add the body
			event = new HashMap<String,String>(headers);
			event.put(this.body, new String(e.getBody()));
		}
		Gson gson = new Gson();
    out.write(gson.toJson(event).getBytes());
    if (appendNewline) {
      out.write('\n');
    }
  }

  @Override
  public void flush() throws IOException {
    // noop
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      JSONEventSerializer s = new JSONEventSerializer(out, context);
      return s;
    }

  }

}
