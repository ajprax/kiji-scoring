/**
 * (c) Copyright 2014 WibiData, Inc.
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
package org.kiji.scoring;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestMapFamilyScoringProducer extends KijiClientTest {
  private static final String LAYOUT_PATH = "test-map-family-freshening.json";
  private static final String TABLE_NAME = "test_map_family_freshening";

  private static final String ENTITY = "foo";
  private static final String MAP = "map";
  private static final String QUAL0 = "qual0";
  private static final String QUAL1 = "qual1";
  private static final String INFO = "info";
  private static final String NAME = "name";
  private static final String FOO_VAL = "foo-val";
  private static final String NEW_VAL = "new-val";

  private static final class TestMapFamilyScoreFunction extends ScoreFunction<String> {
    public KijiDataRequest getDataRequest(
        final FreshenerContext context
    ) throws IOException {
      // getDataRequest should only be called in the context of the entire family.
      Assert.assertTrue(!context.getAttachedColumn().isFullyQualified());
      // Note: the ScoreFunction must request some data that exists in every row, even if it does
      // not use the data, otherwise the row will be skipped.
      return KijiDataRequest.create(INFO, NAME);
    }

    public TimestampedValue<String> score(
        final KijiRowData dataToScore,
        final FreshenerContext context
    ) throws IOException {
      // score should only be called in the context of a qualified column.
      Assert.assertTrue(context.getAttachedColumn().isFullyQualified());
      return TimestampedValue.create(NEW_VAL);
    }
  }

  private static final class TestMapFamilyScoringProducerImpl extends MapFamilyScoringProducer {
    public Set<String> getQualifiersToScore(
        final KijiRowData input,
        final FreshenerContext context
    ) throws IOException {
      // getQualifiersToScore should only be called in the context of the entire family.
      Assert.assertTrue(!context.getAttachedColumn().isFullyQualified());
      // This uses the info:name column because the layout already contains it, normally this would
      // be a column specific for this information.
      final String qualifiers = input.getMostRecentValue(INFO, NAME).toString();
      return Sets.newHashSet(qualifiers.split(","));
    }
  }

  private KijiTable mTable = null;
  private KijiTableReader mReader = null;
  private EntityId mEid = null;

  @Before
  public void setupTestMapFamilyProducer() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(LAYOUT_PATH))
            .withRow(ENTITY)
                .withFamily(MAP)
                    .withQualifier(QUAL0)
                        .withValue(10, FOO_VAL)
                    .withQualifier(QUAL1)
                        .withValue(10, FOO_VAL)
                .withFamily(INFO)
                    .withQualifier(NAME)
                        .withValue(QUAL0 + "," + QUAL1)
        .build();
    mTable = getKiji().openTable(TABLE_NAME);
    mReader = mTable.openTableReader();
    mEid = mTable.getEntityId(ENTITY);
  }

  @After
  public void cleanupTestMapFamilyProducer() throws IOException {
    mReader.close();
    mTable.release();
  }

  @Test
  public void test() throws IOException, InterruptedException, ClassNotFoundException {
    final Configuration conf = getConf();
    conf.set(MapFamilyScoringProducer.ATTACHED_FAMILY_KEY, MAP);
    // These empty parameters could be omitted.
    conf.set(MapFamilyScoringProducer.PARAMETERS_KEY, "{}");
    conf.set(MapFamilyScoringProducer.SCORE_FUNCTION_CLASS_KEY,
        TestMapFamilyScoreFunction.class.getName());
    final KijiMapReduceJob job = KijiProduceJobBuilder.create()
        .withConf(conf)
        .withProducer(TestMapFamilyScoringProducerImpl.class)
        .withInputTable(mTable.getURI())
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    Assert.assertTrue(job.run());
    final KijiRowData data = mReader.get(mEid, KijiDataRequest.create(MAP));
    Assert.assertEquals(NEW_VAL, data.getMostRecentValue(MAP, QUAL0).toString());
    Assert.assertEquals(NEW_VAL, data.getMostRecentValue(MAP, QUAL1).toString());
  }
}
