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
package org.apache.twill.yarn;

import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.twill.api.Hosts;
import org.apache.twill.api.Racks;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.internal.yarn.YarnUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for placement Policies.
 */
public class PlacementPolicyTestRun extends BaseYarnTest {
  private static List<NodeReport> nodeReports;
  private static ResourceSpecification resource;
  private static ResourceSpecification twoInstancesResource;

  /**
   * Verify the cluster configuration (number and capability of node managers) required for the tests.
   */
  @BeforeClass
  public static void verifyClusterCapability() {
    // Ignore verifications if it is running against older Hadoop versions which does not support blacklists.
    Assume.assumeTrue(YarnUtils.getHadoopVersion().equals(YarnUtils.HadoopVersions.HADOOP22));

    // All runnables in this test class use same resource specification for the sake of convenience.
    resource = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .build();
    twoInstancesResource = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(2)
      .build();

    try {
      nodeReports = YarnTestUtils.getNodeReports();
    } catch (Exception e) {
      e.printStackTrace();
    }
    // The tests need exactly three NodeManagers in the cluster.
    Assert.assertNotNull(nodeReports);
    Assert.assertEquals(nodeReports.size(), 3);

    // All NodeManagers should have enough capacity available to accommodate at least two runnables.
    for (NodeReport nodeReport : nodeReports) {
      Resource capability = nodeReport.getCapability();
      Resource used = nodeReport.getUsed();
      Assert.assertNotNull(capability);
      if (used != null) {
        Assert.assertTrue(2 * resource.getMemorySize() < capability.getMemory() - used.getMemory());
      } else {
        Assert.assertTrue(2 * resource.getMemorySize() < capability.getMemory());
      }
    }
  }

  /**
   * Test to verify placement policy without dynamically changing number of instances.
   */
  @Test
  public void testPlacementPolicy() throws InterruptedException {
    // Ignore test if it is running against older Hadoop versions which does not support blacklists.
    Assume.assumeTrue(YarnUtils.getHadoopVersion().equals(YarnUtils.HadoopVersions.HADOOP22));

    ServiceDiscovered serviceDiscovered;
    ResourceReport resourceReport;
    Set<Integer> nmPorts = Sets.newHashSet();
    Collection<TwillRunResources> distributedResource;

    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new PlacementPolicyApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("PlacementPolicyTest")
      .withArguments("hostRunnable", "host")
      .withArguments("hostRackRunnable", "hostRack")
      .withArguments("distributedRunnable", "distributed")
      .start();

    try {
      // All runnables should get started.
      serviceDiscovered = controller.discoverService("PlacementPolicyTest");
      Assert.assertTrue(YarnTestUtils.waitForSize(serviceDiscovered, 4, 80));

      // DISTRIBUTED runnables should be provisioned on different nodes.
      resourceReport = controller.getResourceReport();
      distributedResource = resourceReport.getRunnableResources("distributedRunnable");
      Assert.assertNotNull(distributedResource);
      Assert.assertEquals(distributedResource.size(), 2);
      Iterator<TwillRunResources> distributedResourceIterator = distributedResource.iterator();
      nmPorts.add(distributedResourceIterator.next().getNMPort());
      nmPorts.add(distributedResourceIterator.next().getNMPort());
      Assert.assertEquals(nmPorts.size(), 2);
    } finally {
      controller.stopAndWait();
    }

    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  /**
   * An application that specify runnables with different placement policies.
   */
  public static final class PlacementPolicyApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("PlacementPolicyApplication")
        .withRunnable()
          .add("hostRunnable", new EchoServer(), resource).noLocalFiles()
          .add("hostRackRunnable", new EchoServer(), resource).noLocalFiles()
          .add("distributedRunnable", new EchoServer(), twoInstancesResource).noLocalFiles()
        .withPlacementPolicy()
          .add(Hosts.of(nodeReports.get(0).getHttpAddress()), "hostRunnable")
          .add(Hosts.of(nodeReports.get(1).getHttpAddress()), Racks.of("/default-rack"), "hostRackRunnable")
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "distributedRunnable")
        .anyOrder()
        .build();
    }
  }

  /**
   * Test to verify DISTRIBUTED placement policies are taken care of when number of instances are changed.
   * Also, verifies that DISTRIBUTED placement policies do not affect other runnables.
   */
  @Test
  public void testDistributedPlacementPolicy() throws InterruptedException {
    // Ignore test if it is running against older Hadoop versions which does not support blacklists.
    Assume.assumeTrue(YarnUtils.getHadoopVersion().equals(YarnUtils.HadoopVersions.HADOOP22));

    ServiceDiscovered serviceDiscovered;
    ResourceReport resourceReport;
    Set<Integer> nmPorts = Sets.newHashSet();
    Collection<TwillRunResources> aliceResources;
    Collection<TwillRunResources> bobResources;

    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new DistributedApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("DistributedTest")
      .withArguments("Alice", "alice")
      .withArguments("Bob", "bob")
      .withArguments("Eve", "eve")
      .start();

    try {
      // All runnables should get started with DISTRIBUTED ones being on different nodes.
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(YarnTestUtils.waitForSize(serviceDiscovered, 3, 60));
      resourceReport = controller.getResourceReport();
      Assert.assertNotNull(resourceReport);
      aliceResources = resourceReport.getRunnableResources("Alice");
      Assert.assertNotNull(aliceResources);
      Assert.assertEquals(aliceResources.size(), 1);
      bobResources = resourceReport.getRunnableResources("Bob");
      Assert.assertNotNull(bobResources);
      Assert.assertEquals(bobResources.size(), 1);
      nmPorts.add(aliceResources.iterator().next().getNMPort());
      nmPorts.add(bobResources.iterator().next().getNMPort());
      Assert.assertEquals(nmPorts.size(), 2);

      // Spawning a new instance for DISTRIBUTED runnable Alice, which should get a different node.
      controller.changeInstances("Alice", 2);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(YarnTestUtils.waitForSize(serviceDiscovered, 4, 60));
      resourceReport = controller.getResourceReport();
      Assert.assertNotNull(resourceReport);
      aliceResources = resourceReport.getRunnableResources("Alice");
      Assert.assertNotNull(aliceResources);
      Assert.assertEquals(aliceResources.size(), 2);
      Iterator<TwillRunResources> aliceResourceIterator = aliceResources.iterator();
      nmPorts.add(aliceResourceIterator.next().getNMPort());
      nmPorts.add(aliceResourceIterator.next().getNMPort());
      Assert.assertEquals(nmPorts.size(), 3);

      // Spawning a new instance for DEFAULT runnable Eve,
      // which should not be affected by placement policies of previous runnables.
      controller.changeInstances("Eve", 2);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(YarnTestUtils.waitForSize(serviceDiscovered, 5, 60));

      // Spawning a new instance for DISTRIBUTED runnable Bob,
      // which will be forced to give up it's placement policy restrictions, since there are only three nodes.
      controller.changeInstances("Bob", 2);
      serviceDiscovered = controller.discoverService("DistributedTest");
      Assert.assertTrue(YarnTestUtils.waitForSize(serviceDiscovered, 6, 60));
      resourceReport = controller.getResourceReport();
      Assert.assertNotNull(resourceReport);
      bobResources = resourceReport.getRunnableResources("Bob");
      Assert.assertNotNull(bobResources);
      Assert.assertEquals(bobResources.size(), 2);
      Iterator<TwillRunResources> bobResourceIterator = bobResources.iterator();
      nmPorts.add(bobResourceIterator.next().getNMPort());
      nmPorts.add(bobResourceIterator.next().getNMPort());
      Assert.assertEquals(nmPorts.size(), 3);
    } finally {
      controller.stopAndWait();
    }

    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  /**
   * An application that runs three runnables, with a DISTRIBUTED placement policy for two of them.
   */
  public static final class DistributedApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("DistributedApplication")
        .withRunnable()
          .add("Alice", new EchoServer(), resource).noLocalFiles()
          .add("Bob", new EchoServer(), resource).noLocalFiles()
          .add("Eve", new EchoServer(), resource).noLocalFiles()
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "Alice", "Bob")
        .anyOrder()
        .build();
    }
  }

  /**
   * Test to verify exception is thrown in case a non-existent runnable is specified in a placement policy.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNonExistentRunnable() {
    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new FaultyApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();
      controller.stopAndWait();
  }

  /**
   * An application that uses non-existent runnable name while specifying placement policies.
   */
  public static final class FaultyApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("FaultyApplication")
        .withRunnable()
          .add("Hermione", new EchoServer(), resource).noLocalFiles()
          .add("Harry", new EchoServer(), resource).noLocalFiles()
          .add("Ron", new EchoServer(), resource).noLocalFiles()
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicy.Type.DEFAULT, "Hermione", "Ron")
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "Draco", "Harry")
          .anyOrder()
        .build();
    }
  }

  /**
   * Test to verify exception is thrown in case a runnable is mentioned in more than one placement policy.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testPlacementPolicySpecification() {
    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new BadApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();
      controller.stopAndWait();
  }

  /**
   * An application that specifies a runnable name in more than one placement policy.
   */
  public static final class BadApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("BadApplication")
        .withRunnable()
          .add("Hermione", new EchoServer(), resource).noLocalFiles()
          .add("Harry", new EchoServer(), resource).noLocalFiles()
          .add("Ron", new EchoServer(), resource).noLocalFiles()
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicy.Type.DEFAULT, "Hermione", "Harry")
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, "Hermione", "Ron")
        .anyOrder()
        .build();
    }
  }
}
