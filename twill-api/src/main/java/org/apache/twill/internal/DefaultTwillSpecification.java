/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.PlacementHints;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Straightforward implementation of {@link org.apache.twill.api.TwillSpecification}.
 */
public final class DefaultTwillSpecification implements TwillSpecification {

  private final String name;
  private final Map<String, RuntimeSpecification> runnables;
  private final List<Order> orders;
  private final PlacementPolicy placementPolicy;
  private final EventHandlerSpecification eventHandler;

  public DefaultTwillSpecification(String name, Map<String, RuntimeSpecification> runnables,
                                   List<Order> orders, PlacementPolicy placementPolicy,
                                   EventHandlerSpecification eventHandler) {
    this.name = name;
    this.runnables = ImmutableMap.copyOf(runnables);
    this.orders = ImmutableList.copyOf(orders);
    this.placementPolicy = placementPolicy;
    this.eventHandler = eventHandler;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Map<String, RuntimeSpecification> getRunnables() {
    return runnables;
  }

  @Override
  public List<Order> getOrders() {
    return orders;
  }

  @Override
  public PlacementPolicy getPlacementPolicy() {
    return placementPolicy;
  }

  @Nullable
  @Override
  public EventHandlerSpecification getEventHandler() {
    return eventHandler;
  }

  /**
   * Straightforward implementation of {@link Order}.
   */
  public static final class DefaultOrder implements Order {

    private final Set<String> names;
    private final Type type;

    public DefaultOrder(Iterable<String> names, Type type) {
      this.names = ImmutableSet.copyOf(names);
      this.type = type;
    }

    @Override
    public Set<String> getNames() {
      return names;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("names", names)
        .add("type", type)
        .toString();
    }
  }

  /**
   * Straightforward implementation of {@link org.apache.twill.api.TwillSpecification.PlacementPolicy}.
   */
  public static final class DefaultPlacementPolicy implements PlacementPolicy {

    //Map from GroupId to PlacementGroup
    private Map<Integer, PlacementPolicyGroup> groups;

    public DefaultPlacementPolicy() {
      groups = new HashMap<Integer, PlacementPolicyGroup>();
    }

    public DefaultPlacementPolicy(List<PlacementPolicyGroup> groups) {
      this();
      for (PlacementPolicyGroup group : groups) {
        add(group);
      }
    }

    @Override
    public void add(PlacementPolicyGroup placementPolicyGroup) {
      groups.put(groups.size(), placementPolicyGroup);
    }

    @Override
    public List<PlacementPolicyGroup> getPlacementPolicyGroups() {
      List<PlacementPolicyGroup> placementPolicyGroups = new ArrayList<PlacementPolicyGroup>();
      return ImmutableList.<PlacementPolicyGroup>copyOf(groups.values());
    }

    // Get groups for runnables belonging to given Order
    @Override
    public List<PlacementPolicyGroup> getPlacementPolicyGroups(Order order) {
      return getPlacementPolicyGroups(order.getNames());
    }

    //Get groups for only a particular set of runnables
    @Override
    public List<PlacementPolicyGroup> getPlacementPolicyGroups(Set<String> runnableNames) {
      List<PlacementPolicyGroup> placementPolicyGroups = new ArrayList<PlacementPolicyGroup>();
      Multimap<Integer, String> distributedRunnables = HashMultimap.create();
      Set<String> uncaredRunnables = new HashSet<String>();
      for (String runnableName : runnableNames) {
        int groupId = getPlacementPolicyGroupId(runnableName);
        if (groupId == -1) {
          continue;
        }
        if (groups.get(groupId).getType().equals(PlacementPolicyGroup.Type.UNCARED)) {
          uncaredRunnables.add(runnableName);
        } else if (groups.get(groupId).getType().equals(PlacementPolicyGroup.Type.DISTRIBUTED)) {
          distributedRunnables.put(groupId, runnableName);
        }
      }
      for (Collection runnableCollection : distributedRunnables.asMap().values()) {
        placementPolicyGroups.add(new DefaultPlacementPolicyGroup(runnableCollection,
                                                                  PlacementPolicyGroup.Type.DISTRIBUTED));
      }
      placementPolicyGroups.add(new DefaultPlacementPolicyGroup(uncaredRunnables, PlacementPolicyGroup.Type.UNCARED));
      return placementPolicyGroups;
    }

    @Override
    public PlacementPolicyGroup getPlacementPolicyGroup(String runnableName) {
      int groupId = getPlacementPolicyGroupId(runnableName);
      return (groupId == -1) ? null : getPlacementPolicyGroups().get(groupId);
    }

    //Returns -1 if runnable is not found
    private int getPlacementPolicyGroupId(String runnable) {
      for (int groupId = 0; groupId < groups.size(); groupId++) {
        if (groups.get(groupId).getNames().contains(runnable)) {
          return groupId;
        }
      }
      return -1;
    }

    @Override
    public int size() {
      return groups.size();
    }
  }


  /**
   * Straightforward implementation of {@link org.apache.twill.api.TwillSpecification.PlacementPolicyGroup}.
   */
  public static final class DefaultPlacementPolicyGroup implements PlacementPolicyGroup {

    private final Set<String> names;
    private final Type type;
    private final PlacementHints placementHints;

    public DefaultPlacementPolicyGroup(Iterable<String> names, Type type, PlacementHints placementHints) {
      this.names = ImmutableSet.copyOf(names);
      this.type = type;
      this.placementHints = placementHints;
    }

    public DefaultPlacementPolicyGroup(Iterable<String> names, Type type) {
      this.names = ImmutableSet.copyOf(names);
      this.type = type;
      this.placementHints = new PlacementHints();
    }

    @Override
    public Set<String> getNames() {
      return names;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public PlacementHints getPlacementHints() {
      return placementHints;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("names", names)
        .add("type", type)
        .toString();
    }
  }
}
