package org.elasticsearch.river.rabbitmq.script;

import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.Map;

public class MockScriptFactory implements NativeScriptFactory {
  
  @Override
  public ExecutableScript newScript(Map<String, Object> params) {
    return new MockScript(params);
  }
}
