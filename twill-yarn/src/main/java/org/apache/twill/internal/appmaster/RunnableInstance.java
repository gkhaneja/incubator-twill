package org.apache.twill.internal.appmaster;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.twill.api.ResourceSpecification;

/**
 * Created by gourav on 6/8/14.
 */
public class RunnableInstance {

  String runnableName;
  int instanceId;
  Resource resource;

  public RunnableInstance(String runnableName, int instanceId, Resource resource) {
    this.runnableName = runnableName;
    this.instanceId = instanceId;
    this.resource = resource;
  }

  public Resource getResource() {
    return this.resource;
  }

  public String toString() {
    return runnableName + "-" + instanceId;
  }
}
