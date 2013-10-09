package storm.trident.testing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/*
   * Identity filter that logs a tuple.
   * Useful for logging every tuple in a stream.
   */
  @SuppressWarnings({ "serial", "rawtypes" })
  public class Tap extends BaseFilter {
    private static final Logger LOG = LoggerFactory.getLogger(Tap.class);

    // just print the tuple
      @Override
      public boolean isKeep(TridentTuple tuple){
        LOG.info(Utils.logString("","","","tuple",tuple.toString()));
        return true;
      }
  }

