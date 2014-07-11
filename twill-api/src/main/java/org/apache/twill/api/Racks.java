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
 * Represents a list of Racks.
 */

public class Racks {
  ImmutableList<String> racks;

  public Racks(ImmutableList<String> racks) {
    this.racks = racks;
  }

  /**
   * Creates an instance of {@link org.apache.twill.api.Racks}.
   * @param rack A rack to be added.
   * @param moreRacks A list of racks to be added.
   * @return An instance of {@link org.apache.twill.api.Racks} containing specified racks.
   */
  public static Racks of(String rack, String...moreRacks) {
    return new Racks(new ImmutableList.Builder<String>().add(rack).add(moreRacks).build());
  }

  /**
   * Get the list of racks.
   * @return list of racks.
   */
  public List<String> get() {
    return this.racks;
  }

  public String toString() {
    return this.racks.toString();
  }
}
