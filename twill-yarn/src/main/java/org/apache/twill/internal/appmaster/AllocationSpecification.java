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
package org.apache.twill.internal.appmaster;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.twill.api.RuntimeSpecification;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * This class defines how the containers should be allocated.
 *
 */
public class AllocationSpecification {

  /**
   * Defines different types of allocation strategies.
   */
  enum Type {
    ALLOCATE_ONE_INSTANCE_AT_A_TIME,
    DEFAULT
  }

  /**
   * Resource specification of runnables.
   */
  Resource resource;

  /**
   * Allocation strategy Type.
   */
  Type type;

  /**
   * Name of runnable. Set to null if the class represents more than one runnable.
   */
  String runnableName;

  /**
   * Instance number for the runnable. Set to  0 if the class represents more than one instance / runnable.
   */
  int instanceId;

  public AllocationSpecification(Resource resource) {
    this(resource, Type.DEFAULT, null, 0);
  }

  public AllocationSpecification(Resource resource, Type type, String runnableName, int instanceId) {
    this.resource = resource;
    this.type = type;
    this.runnableName = runnableName;
    this.instanceId = instanceId;
  }

  public Resource getResource() {
    return this.resource;
  }

  public Type getType() {
    return this.type;
  }

  public String getRunnableName() {
    return this.runnableName;
  }

  public int getInstanceId() {
    return this.instanceId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof AllocationSpecification)) {
      return false;
    }
    AllocationSpecification allocationSpecification = (AllocationSpecification) obj;
    if (this.instanceId == allocationSpecification.getInstanceId() &&
      this.resource.equals(allocationSpecification.getResource()) &&
      this.type.equals(allocationSpecification.getType())) {
      if (this.runnableName == null && allocationSpecification.getRunnableName() == null) {
        return true;
      } else if (this.runnableName != null && allocationSpecification.getRunnableName() != null) {
        if (this.runnableName.equals(allocationSpecification.getRunnableName())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Helper method to create {@link org.apache.twill.internal.appmaster.RunnableContainerRequest}.
   * @param allocationSpecification
   * @param map
   * @param runtimeSpec
   */
  public static void addAllocationSpecification(AllocationSpecification allocationSpecification,
                                                Map<AllocationSpecification, Collection<RuntimeSpecification>> map,
                                                RuntimeSpecification runtimeSpec) {
    AllocationSpecification key = getKey(allocationSpecification, map);
    map.get(key).add(runtimeSpec);
  }

  private static AllocationSpecification getKey(AllocationSpecification allocationSpecification,
                                                Map<AllocationSpecification, Collection<RuntimeSpecification>> map) {
    Iterator<AllocationSpecification> iterator = map.keySet().iterator();
    while (iterator.hasNext()) {
      AllocationSpecification key = iterator.next();
      if (allocationSpecification.equals(key)) {
        return key;
      }
    }
    map.put(allocationSpecification, new HashSet<RuntimeSpecification>());
    return allocationSpecification;
  }

}
