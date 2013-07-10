package org.elasticsearch.river.rabbitmq.script;

import org.elasticsearch.common.jackson.core.JsonFactory;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.script.AbstractExecutableScript;

import java.io.*;
import java.util.Map;

public class MockScript extends AbstractExecutableScript {
  
  private final Map<String, Object> params;
  
  public MockScript(Map<String, Object> params) {
    super();
    this.params = params;
  }
  
  @Override
  public void setNextVar(String name, Object value) {
    params.put(name, value);
  }
  
  @Override
  public Object run() {
    String body = (String) params.get("body");
    BufferedReader reader = new BufferedReader(new StringReader(body));
    
    CharArrayWriter charArrayWriter = new CharArrayWriter();
    BufferedWriter writer = new BufferedWriter(charArrayWriter);
    
    try {
      process(reader, writer);
    } catch (IOException e) {
      // TODO: wrap or treat it
      throw new RuntimeException(e);
    }
    
    String outputBody = charArrayWriter.toString();
    System.out.println("input message:\n" + body);
    System.out.println("output message:\n" + outputBody);
    
    return outputBody;
  }

  private void process(BufferedReader reader, BufferedWriter writer) throws IOException {
    JsonFactory factory = new JsonFactory();
    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
      JsonXContentParser parser = new JsonXContentParser(factory.createJsonParser(line));
      Map<String, Object> asMap = parser.map();
      
      if (asMap.get("create") != null) {
        // skip "create" operations, header and body
        reader.readLine();
      } else {
        writer.write(line);
        writer.newLine();
      }
    }
    writer.flush();
    writer.close();
  }
}
