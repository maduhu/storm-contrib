package chimpstorm.storm.trident.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infochimps.storm.trident.util.JsonUtil.*;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonParse extends BaseFunction {

  private final static Logger LOG = LoggerFactory.getLogger(JsonParse.class);
  private final static ObjectMapper jsonMapper = new ObjectMapper();

  public void execute(TridentTuple tuple, TridentCollector collector){
    try {
      JsonNode node = jsonMapper.readTree(tuple.getString(0));
      List values = new ArrayList(1);
      values.add(node);
      collector.emit(values);
    } catch(IOException ioEx) {
      LOG.error("Error parsing json", ioEx);
    }
    return;
  }
}
