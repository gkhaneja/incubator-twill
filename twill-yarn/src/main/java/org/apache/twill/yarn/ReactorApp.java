package org.apache.twill.yarn;

import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Created by gourav on 6/6/14.
 */
public class ReactorApp implements TwillApplication {

  @Override
  public TwillSpecification configure() {



    ResourceSpecification streamSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(2)
      .build();

    ResourceSpecification transactionSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(2)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .build();

    ResourceSpecification metricsSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(2)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .build();

    ResourceSpecification metricsProcessorSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(2)
      .build();

    ResourceSpecification logSaverSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(2)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .build();

    return TwillSpecification.Builder.with()
      .setName("ReactorApp")
      .withRunnable()
      .add("stream", new ResourceRunnable(), streamSpec).noLocalFiles()
      .add("transactionService", new ResourceRunnable(), transactionSpec).noLocalFiles()
      .add("metricsService", new ResourceRunnable(), metricsSpec).noLocalFiles()
      .add("metricsProcessor", new ResourceRunnable(), metricsProcessorSpec).noLocalFiles()
      .add("logSaverService", new ResourceRunnable(), logSaverSpec).noLocalFiles()
      .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicyGroup.Type.DISTRIBUTED, "stream")
      .anyOrder()
      .build();
  }

  private static final class ResourceRunnable extends AbstractTwillRunnable {

    public static Logger logger = LoggerFactory.getLogger(ResourceRunnable.class);
    private volatile Thread runThread;

    @Override
    public void run() {
      runThread = Thread.currentThread();
      Random random = new Random();

      logger.info("Container is going to sleep for 10 seconds.");
      try {
        Thread.currentThread().sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      //getContext().announce("sleep", Integer.parseInt(getContext().getSpecification().getConfigs().get("port")));

    }

    @Override
    public void stop() {
      //No op
    }
  }
}
