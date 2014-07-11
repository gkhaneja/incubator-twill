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
import org.apache.twill.api.Hosts;
import org.apache.twill.api.Racks;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 * Tests for placement Policies.
 */
public class PlacementPolicyTestRun extends BaseYarnTest {
  private static final Logger LOG = LoggerFactory.getLogger(PlacementPolicyTestRun.class);

  @Test
  public void testDefaultPlacementPolicy() throws InterruptedException {
    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new HostsAndRacksApp())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    try {
      ServiceDiscovered discovered1 = controller.discoverService("IMAlive");
      Assert.assertTrue(YarnTestUtils.waitForSize(discovered1, 6, 60));

      //ServiceDiscovered discovered2 = controller.discoverService("rack");
      //Assert.assertTrue(YarnTestUtils.waitForSize(discovered2, 2, 60));
    } finally {
      controller.stopAndWait();
    }
  }

  /*@Test
  public void testPlacementPolicyAPI() throws InterruptedException {

  }*/

  @Test
  public void testDistributedPlacementPolicy() throws InterruptedException {
    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new DistributedApp())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    try {
      ServiceDiscovered discovered1 = controller.discoverService("IMAlive");
      Assert.assertTrue(YarnTestUtils.waitForSize(discovered1, 2, 60));

      //ServiceDiscovered discovered2 = controller.discoverService("distributed");
      //Assert.assertTrue(YarnTestUtils.waitForSize(discovered2, 1, 60));

      //This is a negative test case. Since MiniYArnCluster has only one host,
      //it should not be able to run second DISTRIBUTED runnable.
      //Assert.assertTrue(!YarnTestUtils.waitForSize(discovered2, 2, 60));
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
   * An application that specify hosts and/or racks for runnables. These are just soft suggestions.
   */
  public static final class HostsAndRacksApp implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      ResourceSpecification resource = ResourceSpecification.Builder.with()
        .setVirtualCores(1)
        .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
        .build();

      String localhostIP = null;
      try {
        localhostIP = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }

      return TwillSpecification.Builder.with()
        .setName("DistributedApp")
        .withRunnable()
        .add("hostRunnable1", new SpeakAndSleepRunnable(12344), resource).noLocalFiles()
        .add("hostRunnable2", new SpeakAndSleepRunnable(12345), resource).noLocalFiles()
        .add("rackRunnable1", new SpeakAndSleepRunnable(12346), resource).noLocalFiles()
        .add("rackRunnable2", new SpeakAndSleepRunnable(12347), resource).noLocalFiles()
        .add("hostRackRunnable1", new SpeakAndSleepRunnable(12348), resource).noLocalFiles()
        .add("hostRackRunnable2", new SpeakAndSleepRunnable(12349), resource).noLocalFiles()
        .withPlacementPolicy()
          .add(Hosts.of(localhostIP), "hostRunnable1", "hostRunnable2")
          .add(Racks.of("/default-rack"), "rackRunnable1", "rackRunnable2")
          .add(Hosts.of(localhostIP), Racks.of("/default-rack"), "hostRackRunnable1", "hostRackRunnable2")
        .anyOrder()
        .build();
    }
  }

  /**
   * An application that specify DISTRIBUTED runnables.
   */
  public static final class DistributedApp implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      ResourceSpecification resource = ResourceSpecification.Builder.with()
        .setVirtualCores(1)
        .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
        .build();

      String localhostIP = null;
      try {
        localhostIP = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }

      return TwillSpecification.Builder.with()
        .setName("DistributedApp")
        .withRunnable()
        .add("hostRackRunnable", new SpeakAndSleepRunnable(12344), resource).noLocalFiles()
        .add("distributedRunnable1", new SpeakAndSleepRunnable(12345), resource).noLocalFiles()
        .add("distributedRunnable2", new SpeakAndSleepRunnable(12346), resource).noLocalFiles()
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "distributedRunnable1", "distributedRunnable2")
          .add(Hosts.of(localhostIP), Racks.of("/default-rack"), "hostRackRunnable")
        .anyOrder()
        .build();
    }
  }


  /**
   * A runnable that announces itself on the port and sleeps for 10 seconds.
   */
  public static final class SpeakAndSleepRunnable extends AbstractTwillRunnable {

    private volatile Thread runThread;

    public SpeakAndSleepRunnable(int port) {
      super(ImmutableMap.of("port", Integer.toString(port)));
    }

    @Override
    public void run() {
      runThread = Thread.currentThread();
      getContext().announce("IMAlive",
                            Integer.parseInt(getContext().getSpecification().getConfigs().get("port")));
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
