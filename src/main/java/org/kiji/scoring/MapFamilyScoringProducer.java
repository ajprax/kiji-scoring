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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;

import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.ScoreFunction.TimestampedValue;
import org.kiji.scoring.impl.InternalFreshenerContext;
import org.kiji.scoring.impl.ScoringUtils;

/**
 * KijiProducer implementation which runs a ScoreFunction for qualifiers in a map-type family.
 *
 * <p>
 *   To use, Extend this class and implement
 *   {@link #getQualifiersToScore(org.kiji.schema.KijiRowData, FreshenerContext)} then run a Kiji
 *   produce job with your implementation. An example implementation and usage can be found in
 *   TestMapFamilyScoringProducer.
 * </p>
 *
 * <p>
 *   This producer uses two required Configuration keys, and one optional key.
 *   <ul>
 *     <li>
 *       {@link #SCORE_FUNCTION_CLASS_KEY} must contain the fully qualified class name of the
 *       ScoreFunction class to run in this producer.
 *     </li>
 *     <li>
 *       {@link #ATTACHED_FAMILY_KEY} must contain the name of the family on whose qualifiers to run
 *       the ScoreFunction.
 *     </li>
 *     <li>
 *       {@link #PARAMETERS_KEY} may contain a json serialized string of freshening parameters which
 *       will be used to configure the ScoreFunction.
 *     </li>
 *   </ul>
 * </p>
 *
 * <p>
 *   When using MapFamilyScoringProducer to perform batch scoring, several things must be kept in
 *   mind.
 *   <ul>
 *     <li>
 *       The lifecycle of KijiProducer is slightly different from ScoreFunction. Some methods will
 *       be called in different orders. Specifically,
 *       {@link ScoreFunction#getDataRequest(FreshenerContext)} will be called before
 *       {@link ScoreFunction#setup(FreshenerSetupContext)} and will be called in the context of the
 *       entire map family, rather than a specific qualified column. getDataRequest will be called
 *       once for the entire job and the resulting KijiRowData will be passed to all calls to
 *       {@link ScoreFunction#score(org.kiji.schema.KijiRowData, FreshenerContext)}.
 *     </li>
 *     <li>
 *       Rather than allowing the Kiji MR framework to manage KeyValueStores for this producer, this
 *       producer will manage KeyValueStores for the ScoreFunction it runs. This means that
 *       unconfigured KeyValueStores will not be properly overridden by KeyValueStore configuration
 *       provided on the command line via the kiji produce CLI or via
 *       {@link org.kiji.mapreduce.framework.MapReduceJobBuilder#withStore(String,
 *       org.kiji.mapreduce.kvstore.KeyValueStore)}.
 *     </li>
 *     <li>
 *       While this producer is designed to run a ScoreFunction against columns which may not have
 *       any data in them (the column thus does not exist yet). The Kiji MR framework requires that
 *       at least one requested column contains data or the row will be skipped entirely.
 *     </li>
 *   </ul>
 * </p>
 */
public abstract class MapFamilyScoringProducer extends KijiProducer {

  /** Fully qualified name of the ScoreFunction class to run. */
  public static final String SCORE_FUNCTION_CLASS_KEY =
      "org.kiji.scoring.MapFamilyScoringProducer.score_function_class_key";
  /** Family in which to refresh values. */
  public static final String ATTACHED_FAMILY_KEY =
      "org.kiji.scoring.MapFamilyScoringProducer.attached_column_key";
  /** Configuration parameters for the ScoreFunction. */
  public static final String PARAMETERS_KEY =
      "org.kiji.scoring.MapFamilyScoringProducer.parameters_key";
  /** Gson used to deserialize parameters from json. */
  private static final Gson GSON = new Gson();

  /** Empty constructor to be used via reflection. */
  public MapFamilyScoringProducer() { }

  /** ScoreFunction to run on each qualifier in the family. */
  private ScoreFunction<?> mScoreFunction = null;
  /**
   * Context for the entire family.
   * <p>
   *   Used for getRequiredStores, setup, cleanup, getDataRequest. For score, a new context will be
   *   created for each qualifier.
   * </p>
   */
  private InternalFreshenerContext mFreshenerContext = null;
  /**
   * KeyValueStoreReaderFactory used to create contexts for each qualifier.
   */
  private KeyValueStoreReaderFactory mKvStoreReaderFactory = null;

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public void setConf(
      final Configuration conf
  ) {
    super.setConf(conf);
    final String sfClassName = Preconditions.checkNotNull(
        conf.get(SCORE_FUNCTION_CLASS_KEY),
        "Configuration must include a fully qualified ScoreFunction class name at key: %s",
        SCORE_FUNCTION_CLASS_KEY);

    final String family = Preconditions.checkNotNull(
        conf.get(ATTACHED_FAMILY_KEY),
        "Configuration must include a family name at key: %s",
        ATTACHED_FAMILY_KEY);
    final KijiColumnName column = new KijiColumnName(family, null);

    final String serializedParameters = conf.get(PARAMETERS_KEY);
    final Map<String, String> parameters = (null != serializedParameters)
        ? GSON.fromJson(serializedParameters, Map.class)
        : Collections.emptyMap();

    mScoreFunction = ScoringUtils.scoreFunctionForName(sfClassName);
    mFreshenerContext = InternalFreshenerContext.create(column, parameters);
    mKvStoreReaderFactory =
        KeyValueStoreReaderFactory.create(mScoreFunction.getRequiredStores(mFreshenerContext));
    mFreshenerContext.setKeyValueStoreReaderFactory(mKvStoreReaderFactory);
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    // Instead of having the MR framework manage the KeyValueStores for the producer, the producer
    // will manage the stores for the ScoreFunction.
    return Collections.emptyMap();
  }

  /** {@inheritDoc} */
  @Override
  public void setup(KijiContext context) throws IOException {
    mScoreFunction.setup(mFreshenerContext);
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup(KijiContext context) throws IOException {
    mScoreFunction.cleanup(mFreshenerContext);
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    try {
      return mScoreFunction.getDataRequest(mFreshenerContext);
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    return mFreshenerContext.getAttachedColumn().getName();
  }

  /** {@inheritDoc} */
  @Override
  public void produce(
      final KijiRowData input,
      final ProducerContext context
  ) throws IOException {
    for (String qualifier : getQualifiersToScore(input, mFreshenerContext)) {
      final FreshenerContext qualifierContext = InternalFreshenerContext.create(
          mFreshenerContext.getClientRequest(),
          new KijiColumnName(mFreshenerContext.getAttachedColumn().getFamily(), qualifier),
          mFreshenerContext.getParameters(),
          Collections.<String, String>emptyMap(),
          mKvStoreReaderFactory);
      final TimestampedValue<?> score = mScoreFunction.score(input, qualifierContext);
      context.put(qualifier, score.getTimestamp(), score.getValue());
    }
  }

  /**
   * Get the set of qualifiers to score.
   *
   * @param input KijiRowData corresponding to the ScoreFunction's
   *     {@link ScoreFunction#getDataRequest(FreshenerContext)} method.
   * @param context FreshenerContext for the entire map family.
   * @return the set of qualifiers to score.
   * @throws IOException in case of an error getting the qualifiers.
   */
  public abstract Set<String> getQualifiersToScore(KijiRowData input, FreshenerContext context)
      throws IOException;
}
