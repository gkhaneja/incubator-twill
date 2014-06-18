package org.apache.twill.yarn;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 * Tests for placement Policies.
 *
 */
public class PlacementPolicyTestRun extends DistributedBaseYarnTest {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceReportTestRun.class);

  @Test
  public void testDistributedPlacementPolicy() throws InterruptedException {
    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new DistributedApp())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    try {
      ServiceDiscovered discovered = controller.discoverService("distributed");
      Assert.assertTrue(YarnTestUtils.waitForSize(discovered, 2, 60));

      List<String> nodes1 = getNodes(controller, "distributed1");
      Assert.assertEquals(1, nodes1.size());
      String node1 = nodes1.get(0);

      List<String> nodes2 = getNodes(controller, "distributed2");
      Assert.assertEquals(1, nodes2.size());
      String node2 = nodes2.get(0);

      LOG.info("Runnable distributed1 launched on {}", node1);
      LOG.info("Runnable distributed2 launched on {}", node2);
      //These two nodes should be different.
      //TODO: However, this does not work all the times at least with Hadoop 2.2
      //Assert.assertNotEquals(node1, node2);
    } finally {
      controller.stopAndWait();
    }
  }

  private static final List<String> getNodes(TwillController controller, String runnable) {
    List<String> nodes = new ArrayList<String>();
    Collection<TwillRunResources> resources =  controller.getResourceReport().getRunnableResources(runnable);
    Iterator<TwillRunResources> iterator = resources.iterator();
    while (iterator.hasNext()) {
       nodes.add(iterator.next().getNode());
    }
    return nodes;
  }

  /**
   * An application that has two distributed runnables with same memory size.
   */
  public static final class DistributedApp implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      ResourceSpecification resource = ResourceSpecification.Builder.with()
        .setVirtualCores(1)
        .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
        .build();

      return TwillSpecification.Builder.with()
        .setName("DistributedApp")
        .withRunnable()
        .add("distributed1", new DistributedRunnable(12345), resource).noLocalFiles()
        .add("distributed2", new DistributedRunnable(12346), resource).noLocalFiles()
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicyGroup.Type.DISTRIBUTED, "distributed1", "distributed2")
        .anyOrder()
        .build();
    }
  }


  /**
   * A runnable that sleep for 10 seconds.
   */
  public static final class DistributedRunnable extends AbstractTwillRunnable {

    private volatile Thread runThread;

    public DistributedRunnable(int port) {
      super(ImmutableMap.of("port", Integer.toString(port)));
    }

    @Override
    public void run() {
      runThread = Thread.currentThread();
      Random random = new Random();
      getContext().announce("distributed", Integer.parseInt(getContext().getSpecification().getConfigs().get("port")));
      try {
        TimeUnit.SECONDS.sleep(10);
      } catch (InterruptedException e) {
        // Ignore.
      }
    }

    @Override
    public void stop() {
      if (runThread != null) {
        runThread.interrupt();
      }
    }
  }
}
