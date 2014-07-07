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
 * Represents a list of Racks
 */

public class Racks {
  List<String> racks;

  public Racks(List<String> racks) {
    this.racks = racks;
  }

  /**
   * Creates an instance of {@link org.apache.twill.api.Racks}.
   * @param rack
   * @param moreRacks
   * @return
   */
  public static Racks of(String rack, String...moreRacks) {
    ArrayList<String> racks = new ArrayList<String>();
    racks.add(rack);
    for (String another : moreRacks) {
      racks.add(another);
    }
    return new Racks(racks);
  }

  /**
   * Get the lists racks.
   * @return
   */
  public List<String> get() {
    return ImmutableList.copyOf(racks);
  }

  public String toString() {
    return "" + this.racks;
  }
}
