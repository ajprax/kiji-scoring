/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiji.scoring.lib;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.FreshenerSetupContext;
import org.kiji.scoring.ScoreFunction;

/**
 * A simple example of a ScoreFunction.
 *
 * <p>
 *   To attach this ScoreFunction in a Freshener:
 *   <pre>
 *     kiji fresh --target=kiji://.env/default/table/family:qualifier \
 *         --do=register \
 *         --policy-class=org.kiji.scoring.lib.AlwaysFreshen \
 *         --score-function-class=org.kiji.scoring.lib.ExampleScoreFunction \
 *         --instantiate-classes
 *   </pre>
 *   column family:qualifier should expect a list of integers.
 * </p>
 * <p>
 *   To open a reader that will freshen with this function:
 *   <pre>
 *     final FreshKijiTableReader freshReader = FreshKijiTableReader.Builder.create()
 *         .withTable(myTable)
 *         .build();
 *   </pre>
 * </p>
 */
public class ExampleScoreFunction extends ScoreFunction<List<Integer>> {

  private static final Logger LOG = LoggerFactory.getLogger(ExampleScoreFunction.class);
  private static final KijiDataRequest EMPTY_REQUEST = KijiDataRequest.builder().build();

  /**
   * Statically defined output of {@link #score(org.kiji.schema.KijiRowData,
   * org.kiji.scoring.FreshenerContext)}.
   */
  private static final List<Integer> OUTPUT = Lists.newArrayList(1, 2, 3, 5);

  // attachment time methods -----------------------------------------------------------------------

  @Override
  public Map<String, String> serializeToParameters() {
    return ImmutableMap.<String, String>builder().put("dummy-key", "dummy-value").build();
  }

  // one-time setup and cleanup methods ------------------------------------------------------------

  @Override
  public void setup(
      final FreshenerSetupContext context
  ) {
    LOG.debug("Parameters visible in setup: {}", context.getParameters());
  }

  @Override
  public void cleanup(
      final FreshenerSetupContext context
  ) {
    LOG.debug("Parameters visible in cleanup: {}", context.getParameters());
  }

  // per-request methods ---------------------------------------------------------------------------

  @Override
  public KijiDataRequest getDataRequest(
      final FreshenerContext context
  ) throws IOException {
    // This example requires no data from the table.
    return EMPTY_REQUEST;
  }

  @Override
  public TimestampedValue<List<Integer>> score(
      final KijiRowData dataToScore,
      final FreshenerContext context
  ) throws IOException {
    LOG.debug("Parameters visible in score: {}", context.getParameters());

    return TimestampedValue.create(OUTPUT);
  }
}
