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

package org.apache.twill.api;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a list of hosts.
 */

public class Hosts {
  ImmutableList<String> hosts;

  public Hosts(ImmutableList<String> hosts) {
    this.hosts = hosts;
  }

  /**
   * Creates an instance of {@link org.apache.twill.api.Hosts}.
   * @param host A host to be added.
   * @param moreHosts A list of hosts to be added.
   * @return An instance of {@link org.apache.twill.api.Hosts} containing specified hosts.
   */
  public static Hosts of(String host, String...moreHosts) {
    return new Hosts(new ImmutableList.Builder<String>().add(host).add(moreHosts).build());
  }

  /**
   * Get the list of hosts.
   * @return list of hosts.
   */
  public List<String> get() {
    return this.hosts;
  }

  public String toString() {
    return this.hosts.toString();
  }


}
