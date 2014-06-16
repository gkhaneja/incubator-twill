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
package org.apache.twill.internal.json;

import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.PlacementHints;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.DefaultEventHandlerSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of gson serializer/deserializer {@link org.apache.twill.api.TwillSpecification}.
 */
final class TwillSpecificationCodec implements JsonSerializer<TwillSpecification>,
                                               JsonDeserializer<TwillSpecification> {

  @Override
  public JsonElement serialize(TwillSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("name", src.getName());
    json.add("runnables", context.serialize(src.getRunnables(),
                                            new TypeToken<Map<String, RuntimeSpecification>>() { }.getType()));
    json.add("orders", context.serialize(src.getOrders(),
                                         new TypeToken<List<TwillSpecification.Order>>() { }.getType()));
    json.add("placementPolicy", context.serialize(src.getPlacementPolicy(),
                                         new TypeToken<TwillSpecification.PlacementPolicy>() { }.getType()));
    EventHandlerSpecification eventHandler = src.getEventHandler();
    if (eventHandler != null) {
      json.add("handler", context.serialize(eventHandler, EventHandlerSpecification.class));
    }

    return json;
  }

  @Override
  public TwillSpecification deserialize(JsonElement json, Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    Map<String, RuntimeSpecification> runnables = context.deserialize(
      jsonObj.get("runnables"), new TypeToken<Map<String, RuntimeSpecification>>() { }.getType());
    List<TwillSpecification.Order> orders = context.deserialize(
      jsonObj.get("orders"), new TypeToken<List<TwillSpecification.Order>>() { }.getType());
    TwillSpecification.PlacementPolicy placementPolicy = context.deserialize(
      jsonObj.get("placementPolicy"), new TypeToken<TwillSpecification.PlacementPolicy>() { }.getType());

    JsonElement handler = jsonObj.get("handler");
    EventHandlerSpecification eventHandler = null;
    if (handler != null && !handler.isJsonNull()) {
      eventHandler = context.deserialize(handler, EventHandlerSpecification.class);
    }

    return new DefaultTwillSpecification(name, runnables, orders, placementPolicy, eventHandler);
  }

  static final class TwillSpecificationOrderCoder implements JsonSerializer<TwillSpecification.Order>,
                                                             JsonDeserializer<TwillSpecification.Order> {

    @Override
    public JsonElement serialize(TwillSpecification.Order src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.add("names", context.serialize(src.getNames(), new TypeToken<Set<String>>() { }.getType()));
      json.addProperty("type", src.getType().name());
      return json;
    }

    @Override
    public TwillSpecification.Order deserialize(JsonElement json, Type typeOfT,
                                                JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();

      Set<String> names = context.deserialize(jsonObj.get("names"), new TypeToken<Set<String>>() { }.getType());
      TwillSpecification.Order.Type type = TwillSpecification.Order.Type.valueOf(jsonObj.get("type").getAsString());

      return new DefaultTwillSpecification.DefaultOrder(names, type);
    }
  }

  static final class TwillSpecificationPlacementPolicyCoder implements
    JsonSerializer<TwillSpecification.PlacementPolicy>, JsonDeserializer<TwillSpecification.PlacementPolicy> {

    @Override
    public JsonElement serialize(TwillSpecification.PlacementPolicy src, Type typeOfSrc,
                                 JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.add("groups", context.serialize(src.getPlacementPolicyGroups(),
                                           new TypeToken<List<TwillSpecification.PlacementPolicyGroup>>() { }
                                                                                                      .getType()));
      return json;
    }

    @Override
    public TwillSpecification.PlacementPolicy deserialize(JsonElement json, Type typeOfT,
                                                          JsonDeserializationContext context)
      throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      List<TwillSpecification.PlacementPolicyGroup> groups = context.deserialize(
        jsonObj.get("groups"), new TypeToken<List<TwillSpecification.PlacementPolicyGroup>>() { }.getType());
      return new DefaultTwillSpecification.DefaultPlacementPolicy(groups);
    }
  }

  static final class TwillSpecificationPlacementPolicyGroupCoder implements
    JsonSerializer<TwillSpecification.PlacementPolicyGroup>, JsonDeserializer<TwillSpecification.PlacementPolicyGroup> {

    @Override
    public JsonElement serialize(TwillSpecification.PlacementPolicyGroup src,
                                 Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.add("names", context.serialize(src.getNames(), new TypeToken<Set<String>>() { }.getType()));
      json.addProperty("type", src.getType().name());
      return json;
    }

    @Override
    public TwillSpecification.PlacementPolicyGroup deserialize(JsonElement json, Type typeOfT,
                                                JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      Set<String> names = context.deserialize(jsonObj.get("names"), new TypeToken<Set<String>>() { }.getType());
      TwillSpecification.PlacementPolicyGroup.Type type =
                              TwillSpecification.PlacementPolicyGroup.Type.valueOf(jsonObj.get("type").getAsString());
      return new DefaultTwillSpecification.DefaultPlacementPolicyGroup(names, type, new PlacementHints());
    }
  }

  static final class EventHandlerSpecificationCoder implements JsonSerializer<EventHandlerSpecification>,
                                                               JsonDeserializer<EventHandlerSpecification> {

    @Override
    public JsonElement serialize(EventHandlerSpecification src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.addProperty("classname", src.getClassName());
      json.add("configs", context.serialize(src.getConfigs(), new TypeToken<Map<String, String>>() { }.getType()));
      return json;
    }

    @Override
    public EventHandlerSpecification deserialize(JsonElement json, Type typeOfT,
                                                 JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      String className = jsonObj.get("classname").getAsString();
      Map<String, String> configs = context.deserialize(jsonObj.get("configs"),
                                                        new TypeToken<Map<String, String>>() {
                                                        }.getType());

      return new DefaultEventHandlerSpecification(className, configs);
    }
  }
}
