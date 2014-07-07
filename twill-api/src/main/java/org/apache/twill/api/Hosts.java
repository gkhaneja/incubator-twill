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
 * Represents a list of hosts
 */

public class Hosts {
  List<String> hosts;

  public Hosts(List<String> hosts) {
    this.hosts = hosts;
  }

  /**
   * Creates an instance of {@link org.apache.twill.api.Hosts}.
   * @param host
   * @param moreHosts
   * @return
   */
  public static Hosts of(String host, String...moreHosts) {
    ArrayList<String> hosts = new ArrayList<String>();
    hosts.add(host);
    for (String another : moreHosts) {
      hosts.add(another);
    }
    return new Hosts(hosts);
  }

  /**
   * Get the list of hosts.
   * @return
   */
  public List<String> get() {
    return ImmutableList.copyOf(hosts);
  }

  public String toString() {
    return "" + this.hosts;
  }


}
