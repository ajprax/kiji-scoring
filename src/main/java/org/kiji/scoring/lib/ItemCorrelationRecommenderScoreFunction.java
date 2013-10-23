package org.kiji.scoring.lib;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.FreshenerGetStoresContext;
import org.kiji.scoring.ScoreFunction;

/**
 * Simple implementation of a ScoreFunction to recommend an item based on correlation with other
 * items.
 *
 * <p>
 *   To attach this ScoreFunction in a Freshener:
 *   <pre>
 *     kiji fresh --target=kiji://.env/default/table/family:qualifier \
 *         --do=register \
 *         --policy-class=my.custom.policy \
 *         --score-function-class=org.kiji.scoring.lib.ItemCorrelationRecommenderScoreFunction
 *         --parameters={"org.kiji.scoring.lib.ItemCorrelationRecommenderScoreFunction.kv_parameter_key":"candidates","org.kiji.scoring.lib.ItemCorrelationRecommenderScoreFunction.number_of_recs_parameter_key":"5"} \
 *   </pre>
 *   my.custom.policy must include a KeyValueStore named "candidates" which maps from String to List
 *   of {@link ItemItemCorrelation} to mask the unconfigured KeyValueStore below.
 * </p>
 *
 * <p>
 *   At request time {@link org.kiji.scoring.FreshKijiTableReader.FreshRequestOptions} should be
 *   used to pass the contents of the cart to the ScoreFunction through the key
 *   {@link #CART_PARAMETER_KEY}.
 * </p>
 *
 * <p>
 *   This ScoreFunction assumes that item correlation has been calculated in batch so that only the
 *   lookup cost is payed by the KVStore.
 * </p>
 */
public class ItemCorrelationRecommenderScoreFunction extends ScoreFunction {

  /**
   * The value stored at this key should be the name of the KVStore which will transform item names
   * into a list of correlated items and their correlated weights. This parameter should be set at
   * attachment time.
   */
  public static final String KVSTORE_NAME_PARAMETER_KEY =
      "org.kiji.scoring.lib.ItemCorrelationRecommenderScoreFunction.kv_parameter_key";
  /**
   * The value stored at this key should be a string representation of an integer for the number of
   * recommendations this ScoreFunction will return. This parameter may be set at attachment or
   * request time. This specifies the maximum, not the exact number of recommendations which will be
   * returned.
   */
  public static final String NUMBER_OF_RECS_PARAMETER_KEY =
      "org.kiji.scoring.lib.ItemCorrelationRecommenderScoreFunction.number_of_recs_parameter_key";
  /**
   * The value stored at this key should be a JSON serialized list of item names from which to
   * recommend other items. This parameter should be set at request time using
   * {@link org.kiji.scoring.FreshKijiTableReader.FreshRequestOptions}.
   */
  public static final String CART_PARAMETER_KEY =
      "org.kiji.scoring.lib.ItemCorrelationRecommenderScoreFunction.cart_parameter_key";

  private static final Gson GSON = new Gson();
  private static final KijiDataRequest EMPTY_REQUEST = KijiDataRequest.builder().build();

  /** Container representing the weight of the correlation between two items. */
  public static final class ItemItemCorrelation implements Comparable<ItemItemCorrelation> {
    private final String mFromItemName;
    private final String mToItemName;
    private final Double mCorrelation;

    public ItemItemCorrelation(
        final String fromItemName,
        final String toItemName,
        final Double correlation
    ) {
      mFromItemName = fromItemName;
      mToItemName = toItemName;
      mCorrelation = correlation;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(
        final ItemItemCorrelation that
    ) {
      return this.mCorrelation.compareTo(that.mCorrelation);
    }
  }

  /** A Function which extracts the correlated item from an ItemItemCorrelation. */
  public static final class ItemItemCorrelationToCorrelatedItemFunction
      implements Function<ItemItemCorrelation, String> {

    /** {@inheritDoc} */
    @Override
    public String apply(
        final ItemItemCorrelation input
    ) {
      return input.mToItemName;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores(
      final FreshenerGetStoresContext context
  ) {
    final Map<String, KeyValueStore<?, ?>> stores = Maps.newHashMap();
    // This KVStore must be overridden by a store of the same name configured by the
    // KijiFreshnessPolicy.
    stores.put(context.getParameter(KVSTORE_NAME_PARAMETER_KEY), UnconfiguredKeyValueStore.get());
    return stores;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest(
      final FreshenerContext context
  ) throws IOException {
    // This ScoreFunction requires no data about the user.
    return EMPTY_REQUEST;
  }

  /** {@inheritDoc} */
  @Override
  public TimestampedValue score(
      final KijiRowData dataToScore,
      final FreshenerContext context
  ) throws IOException {
    // Get the KVStore that will retrieve candidate items.
    final KeyValueStoreReader<String, List<ItemItemCorrelation>> kvReader =
        context.getStore(context.getParameter(KVSTORE_NAME_PARAMETER_KEY));
    // Get the item cart from the context.
    final List<String> cart = GSON.fromJson(context.getParameter(CART_PARAMETER_KEY), List.class);
    // Union the candidates from all cart items.
    final List<ItemItemCorrelation> candidates = Lists.newArrayList();
    for (String item : cart) {
      candidates.addAll(kvReader.get(item));
    }
    // Sort the unioned candidates by their correlation weight.
    Collections.sort(candidates);
    // Transform the correlations to simple item recommendations.
    final List<String> recommendations =
        Lists.transform(candidates, new ItemItemCorrelationToCorrelatedItemFunction());
    // Remove items which are already in the cart from the recommendations.
    for (String item : recommendations) {
      if (cart.contains(item)) {
        recommendations.remove(item);
      }
    }
    // Return the appropriate number.
    final int numberOfReccomendations =
        Integer.valueOf(context.getParameter(NUMBER_OF_RECS_PARAMETER_KEY));
    if (numberOfReccomendations <= recommendations.size()) {
      return TimestampedValue.create(recommendations.subList(0, numberOfReccomendations));
    } else {
      return TimestampedValue.create(recommendations);
    }
  }
}
