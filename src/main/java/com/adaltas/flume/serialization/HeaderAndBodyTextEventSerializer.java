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
 * This class simply writes the header properties and body of the event to the output stream
 * and appends a newline after each event. The "columns" configuration allows
 * to list and order the columns to write. The "format" configuration accept "NATIVE" 
 * and "CSV". In the case of the "CSV" serialization, the fields are space delimited 
 * and the implementation is extremely simple without any escaping.
 */
public class HeaderAndBodyTextEventSerializer implements EventSerializer {

  private final static Logger logger =
      LoggerFactory.getLogger(HeaderAndBodyTextEventSerializer.class);

  // for legacy reasons, by default, append a newline to each event written out
  private final String APPEND_NEWLINE = "appendNewline";
  private final boolean APPEND_NEWLINE_DFLT = true;
  private final String COLUMNS = "columns";
  private final String COLUMNS_DFLT = null;
  private final String FORMAT = "format";
  private final String FORMAT_DFLT = "NATIVE";

  private final OutputStream out;
  private final boolean appendNewline;
  private final String columns;
  private final String format;

  private HeaderAndBodyTextEventSerializer(OutputStream out, Context ctx) {
    this.appendNewline = ctx.getBoolean(APPEND_NEWLINE, APPEND_NEWLINE_DFLT);
    this.columns = ctx.getString(COLUMNS, COLUMNS_DFLT);
    this.format = ctx.getString(FORMAT, FORMAT_DFLT);
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
		Map<String,String> originalHeaders = e.getHeaders();
		Map<String,String> headers;
		if(this.columns != null){
			headers = new LinkedHashMap<String,String>();
			// Keys to keep
			StringTokenizer tok = new StringTokenizer(this.columns);
			while(tok.hasMoreTokens()){
				String key = tok.nextToken();
				headers.put(key, originalHeaders.get(key));
			}
		}else{
			// If json, we need a copy since we'll add the body
			headers = originalHeaders;
		}
		if(this.format == "NATIVE"){
			out.write((headers + " ").getBytes());
	    out.write(e.getBody());
		}else if(this.format == "CSV"){
	    for(Map.Entry<String, String> entry : headers.entrySet()){
	    	out.write(entry.getValue().getBytes());
	    	out.write(',');
	    }
	    out.write(e.getBody());
		}
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
      HeaderAndBodyTextEventSerializer s = new HeaderAndBodyTextEventSerializer(out, context);
      return s;
    }

  }

}
