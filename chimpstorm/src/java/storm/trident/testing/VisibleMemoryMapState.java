package storm.trident.testing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import storm.trident.state.ITupleCollection;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import storm.trident.testing.MemoryMapState;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.*;
import storm.trident.state.snapshot.Snapshottable;

public class VisibleMemoryMapState<T> extends MemoryMapState<T> {
  static final Logger LOG = LoggerFactory.getLogger(VisibleMemoryMapState.class);
  
  public VisibleMemoryMapState(String id) {
    super(id);
    super._backing = new VisibleMemoryMapStateBacking(id);
    super._delegate = new SnapshottableMap(OpaqueMap.build(super._backing), new Values("$MEMORY-MAP-STATE-GLOBAL$"));
    LOG.info(Utils.logString("new MemMState", id, ""));
  }

  public static class Factory implements StateFactory {
    String _id;
    public Factory() {
  	  _id = UUID.randomUUID().toString();
    }
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
      return new VisibleMemoryMapState(_id + partitionIndex);
    }
  }

  public static class VisibleMemoryMapStateBacking<T> extends MemoryMapStateBacking<T> {
    String _id;

    public VisibleMemoryMapStateBacking(String id){
      super(id);
      _id = id;
      LOG.info(Utils.logString("new MMSBacking", _id, ""));
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(Utils.logString("multiGet", _id, "","size", ""+keys.size(), "keys", keys.toString()));
      }
      return super.multiGet(keys);
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
      if (LOG.isDebugEnabled()) {
	  LOG.debug(Utils.logString("multiPut", _id, "","size", ""+keys.size(), "keys", keys.toString(), "vals", vals.toString()));
      }
      super.multiPut(keys, vals);
    }
  }
}
