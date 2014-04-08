package org.kiji.scoring.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReader;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.ScoreFunction.TimestampedValue;
import org.kiji.scoring.impl.MultiBufferedWriter.SingleBuffer;

public class MapFamilyFreshenerCallable implements Callable<Boolean> {

  private final FresheningRequestContext mRequestContext;
  private final KijiColumnName mFamily;
  private final Future<KijiRowData> mClientDataFuture;
  private final Map<KijiColumnName, FreshenerContext> mQualifiersContexts;

  public MapFamilyFreshenerCallable(
      final FresheningRequestContext requestContext,
      final KijiColumnName family,
      final Future<KijiRowData> clientDataFuture
  ) {
    mRequestContext = requestContext;
    mFamily = family;
    mClientDataFuture = clientDataFuture;
    mQualifiersContexts = Maps.newHashMap();
    final Freshener freshener = mRequestContext.getFresheners().get(family);
    for (KijiColumnName qualifier :
        ScoringUtils.getMapFamilyQualifiers(requestContext.getClientDataRequest(), family)) {
      mQualifiersContexts.put(qualifier, InternalFreshenerContext.create(
          mRequestContext.getClientDataRequest(),
          qualifier,
          freshener.getParameters(),
          mRequestContext.getParameterOverrides(),
          freshener.getKVStoreReaderFactory()));
    }
  }

  private Future<KijiRowData> getDataToCheck(
      final Freshener freshener,
      final FreshenerContext context
  ) throws IOException {
    if (freshener.getFreshnessPolicy().shouldUseClientDataRequest(context)) {
      return mClientDataFuture;
    } else {
      final KijiTableReader reader = ScoringUtils.getPooledReader(mRequestContext.getReaderPool());
      try {
        KijiDataRequest policyRequest = KijiDataRequest.empty();
        for (Map.Entry<KijiColumnName, FreshenerContext> qualifierContext
            : mQualifiersContexts.entrySet()) {
          policyRequest = policyRequest.merge(
              freshener.getFreshnessPolicy().getDataRequest(qualifierContext.getValue()));
        }
        return ScoringUtils.getFuture(
            mRequestContext.getExecutorService(),
            new TableReadCallable(
                mRequestContext.getReaderPool(),
                mRequestContext.getEntityId(),
                policyRequest));
      } finally {
        reader.close();
      }
    }
  }

  private KijiRowData getDataToScore(
      final Freshener freshener,
      final Map<KijiColumnName, FreshenerContext> staleQualifiers
  ) throws IOException {
    KijiDataRequest request = KijiDataRequest.empty();
    for (Map.Entry<KijiColumnName, FreshenerContext> staleQualifier : staleQualifiers.entrySet()) {
      request = request.merge(
          freshener.getScoreFunction().getDataRequest(staleQualifier.getValue()));
    }
    final KijiTableReader reader = ScoringUtils.getPooledReader(mRequestContext.getReaderPool());
    try {
      return reader.get(mRequestContext.getEntityId(), request);
    } finally {
      reader.close();
    }
  }

  private static final class ScoreCallable implements Callable<Boolean> {

    final Freshener mFreshener;
    final KijiRowData mDataToScore;
    final KijiColumnName mStaleQualifier;
    final FreshenerContext mContext;
    final FresheningRequestContext mRequestContext;

    private ScoreCallable(
        final Freshener freshener,
        final KijiRowData dataToScore,
        final KijiColumnName staleQualifier,
        final FreshenerContext context,
        final FresheningRequestContext requestContext
    ) {
      mFreshener = freshener;
      mDataToScore = dataToScore;
      mStaleQualifier = staleQualifier;
      mContext = context;
      mRequestContext = requestContext;
    }

    @Override
    public Boolean call() throws Exception {
      final SingleBuffer buffer;
      if (mRequestContext.allowsPartial()) {
        buffer = mRequestContext.openUniqueBuffer();
      } else {
        buffer = mRequestContext.getRequestBuffer();
      }
      final TimestampedValue<?> score = mFreshener.getScoreFunction().score(mDataToScore, mContext);
      buffer.put(
          mRequestContext.getEntityId(),
          mStaleQualifier.getFamily(),
          mStaleQualifier.getQualifier(),
          score.getTimestamp(),
          score.getValue());
      mRequestContext.freshenerWrote();
      final int remainingFresheners = mRequestContext.finishFreshener(mStaleQualifier, true);
      if (mRequestContext.allowsPartial()) {
        buffer.flush();
        return true;
      } else {
        if (0 == remainingFresheners) {
          mRequestContext.getRequestBuffer().flush();
          return true;
        } else {
          return false;
        }
      }
    }
  }

  @Override
  public Boolean call() throws Exception {
    final Freshener freshener = mRequestContext.getFresheners().get(mFamily);

    final KijiRowData dataToCheck = ScoringUtils.getFromFuture(getDataToCheck(
        freshener,
        InternalFreshenerContext.create(
            mRequestContext.getClientDataRequest(),
            mFamily,
            freshener.getParameters(),
            mRequestContext.getParameterOverrides(),
            freshener.getKVStoreReaderFactory())));
    final Map<KijiColumnName, FreshenerContext> staleQualifiers = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, FreshenerContext> qualifierContext
        : mQualifiersContexts.entrySet()) {
      if (!freshener.getFreshnessPolicy().isFresh(dataToCheck, qualifierContext.getValue())) {
        staleQualifiers.put(qualifierContext.getKey(), qualifierContext.getValue());
      }
    }
    if (staleQualifiers.isEmpty()) {
      return false;
    } else {
      final KijiRowData dataToScore = getDataToScore(freshener, staleQualifiers);
      final List<Future<Boolean>> qualifierFutures = Lists.newArrayList();
      for (Map.Entry<KijiColumnName, FreshenerContext> staleQualifier
          : staleQualifiers.entrySet()) {
        final Future<Boolean> qualifierFuture = ScoringUtils.getFuture(
            mRequestContext.getExecutorService(),
            new ScoreCallable(
                freshener,
                dataToScore,
                staleQualifier.getKey(),
                staleQualifier.getValue(),
                mRequestContext));
        qualifierFutures.add(qualifierFuture);
      }
      return ScoringUtils.getFromFuture(ScoringUtils.getFuture(
          mRequestContext.getExecutorService(),
          new FutureAggregatingCallable<Boolean>(qualifierFutures))).contains(true);
    }
  }
}
