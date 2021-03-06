/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.state.spillable;

import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.lib.state.spillable.inmem.InMemSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

public class SpillableComplexComponentImplTest
{
  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  @Test
  public void simpleIntegrationTest()
  {
    InMemSpillableStateStore store = new InMemSpillableStateStore();

    simpleIntegrationTestHelper(store);
  }

  @Test
  public void simpleIntegrationManagedStateTest()
  {
    simpleIntegrationTestHelper(testMeta.store);
  }
  
  public void simpleIntegrationTestHelper(SpillableStateStore store)
  {
    SpillableComplexComponentImpl sccImpl = new SpillableComplexComponentImpl(store);

    Spillable.SpillableComponent scList =
        (Spillable.SpillableComponent)sccImpl.newSpillableArrayList(0L, new SerdeStringSlice());
    Spillable.SpillableComponent scMap =
        (Spillable.SpillableComponent)sccImpl.newSpillableByteMap(0L, new SerdeStringSlice(), new SerdeStringSlice());

    sccImpl.setup(testMeta.operatorContext);

    sccImpl.beginWindow(0L);

    sccImpl.endWindow();

    sccImpl.teardown();
  }
}
