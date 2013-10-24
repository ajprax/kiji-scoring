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
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.FreshenerGetStoresContext;
import org.kiji.scoring.FreshenerSetupContext;
import org.kiji.scoring.ScoreFunction;

/**
 * ScoreFunction which provides personalized recommendations based on item correlations and a users
 * purchase history.
 *
 * <p>
 *   This Recommender assumes that item-item correlation and user-item correlation have been
 *   calculated in batch. It retrieves the items most highly correlated with items in a cart then
 *   ranks them by the user's correlation with those candidates. The list of candidates is provided
 *   by a KVStore which may be use any backing implementation. The user weights for those candidates
 *   are retrieved from a map type family whose qualifiers are product names. The family name should
 *   be stored in parameter key {@link #USER_TABLE_PRODUCT_MAP_FAMILY_PARAMETER_KEY}. In order to
 *   retrieve data from this row the ScoreFunction uses a reader which is opened against the table
 *   whose KijiURI is stored in parameter key {@link #USER_TABLE_URI_PARAMETER_KEY}.
 * </p>
 *
 * <p>
 *   To attach this ScoreFunction in a Freshener.
 *   <pre>
 *     kiji fresh --target=kiji://.env/default/table/family:qualifier \
 *         --do=register \
 *         --policy-class=my.custom.policy \
 *         --score-function-class=org.kiji.scoring.lib.PersonalizedItemCorrelationRecommenderScoreFunction \
 *         --parameters={ \
 *         "org.kiji.scoring.lib.PersonalizedItemCorrelationRecommenderScoreFunction.kv_name_key":"candidates", \
 *         "org.kiji.scoring.lib.PersonalizedItemCorrelationRecommenderScoreFunction.user_table_uri_key":"kiji://.env/default/table", \
 *         "org.kiji.scoring.lib.PersonalizedItemCorrelationRecommenderScoreFunction.product_family_key":"product_correlation", \
 *         "org.kiji.scoring.lib.ItemCorrelationRecommenderScoreFunction.number_of_recs_parameter_key":"5"}
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
 */
public class PersonalizedItemCorrelationRecommenderScoreFunction extends ScoreFunction {

  /**
   * The value stored at this key should be the name of the KVStore which will transform item names
   * into a list of correlated items and their associated correlation weights. This parameter should
   * be set at attachment time.
   */
  public static final String CANDIDATES_KVSTORE_NAME_PARAMETER_KEY =
      "org.kiji.scoring.lib.PersonalizedItemCorrelationRecommenderScoreFunction.kv_name_key";
  /**
   * The value stored at this key should be the string representation of the KijiURI of the user
   * table which has a map type family prepopulated with recommendation scores for all products
   * given the user's profile.
   */
  public static final String USER_TABLE_URI_PARAMETER_KEY =
      "org.kiji.scoring.lib.PersonalizedItemCorrelationRecommenderScoreFunction.user_table_uri_key";
  /**
   * The value stored at this key should be the name of the map type family in the user table which
   * holds prepopulated recommendation scores for all products given the user's profile. The
   * qualifiers in this family should be product names.
   */
  public static final String USER_TABLE_PRODUCT_MAP_FAMILY_PARAMETER_KEY =
      "org.kiji.scoring.lib.PersonalizedItemCorrelationRecommenderScoreFunction.product_family_key";
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

  /** Container representing the weight of a possible item recommendation for a given user. */
  public static final class UserItemCorrelation implements Comparable<UserItemCorrelation> {
    private final String mUserName;
    private final String mItemName;
    private final Double mCorrelation;

    public UserItemCorrelation(
        final String userName,
        final String itemName,
        final Double correlation
    ) {
      mUserName = userName;
      mItemName = itemName;
      mCorrelation = correlation;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(
        final UserItemCorrelation that
    ) {
      return this.mCorrelation.compareTo(that.mCorrelation);
    }
  }

  /** A Function which extracts the correlated item from a UserItemCorrelation. */
  public static final class UserItemCorrelationToCorrelatedItemFunction
      implements Function<UserItemCorrelation, String> {

    @Override
    public String apply(
        final UserItemCorrelation input
    ) {
      return input.mItemName;
    }
  }

  // This should become a reader pool when it is available.
  private KijiTableReader mReader;

  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores(
      final FreshenerGetStoresContext context
  ) {
    final Map<String, KeyValueStore<?, ?>> stores = Maps.newHashMap();
    // This KVStore must be overridden by a store of the same name configured by the
    // KijiFreshnessPolicy.
    stores.put(context.getParameter(CANDIDATES_KVSTORE_NAME_PARAMETER_KEY),
        UnconfiguredKeyValueStore.get());
    return stores;
  }

  @Override
  public void setup(
      final FreshenerSetupContext context
  ) throws IOException {
    final KijiURI uri =
        KijiURI.newBuilder(context.getParameter(USER_TABLE_URI_PARAMETER_KEY)).build();
    final Kiji kiji = Kiji.Factory.open(uri);
    try {
      final KijiTable table = kiji.openTable(uri.getTable());
      try {
        mReader = table.openTableReader();
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

  @Override
  public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
    return EMPTY_REQUEST;
  }

  @Override
  public TimestampedValue score(
      final KijiRowData dataToScore, final FreshenerContext context
  ) throws IOException {
    // Get the KVStore that will retrieve candidate items.
    final KeyValueStoreReader<String, List<ItemItemCorrelation>> candidatesReader =
        context.getStore(context.getParameter(CANDIDATES_KVSTORE_NAME_PARAMETER_KEY));
    // Get the item cart from the context.
    final List<String> cart = GSON.fromJson(context.getParameter(CART_PARAMETER_KEY), List.class);
    // Union the candidates from all cart items.
    final List<ItemItemCorrelation> candidates = Lists.newArrayList();
    for (String item : cart) {
      candidates.addAll(candidatesReader.get(item));
    }
    // From all the candidate products, get the precalculated UserItemCorrelation.
    final String productFamily = context.getParameter(USER_TABLE_PRODUCT_MAP_FAMILY_PARAMETER_KEY);
    final KijiDataRequestBuilder candidateRequestBuilder = KijiDataRequest.builder();
    for (String candidate
        : Lists.transform(candidates, new ItemItemCorrelationToCorrelatedItemFunction())) {
      candidateRequestBuilder.newColumnsDef().withMaxVersions(1).add(productFamily, candidate);
    }
    final KijiDataRequest candidatesWeightsRequest = candidateRequestBuilder.build();
    final KijiRowData candidateWeights =
        mReader.get(dataToScore.getEntityId(), candidatesWeightsRequest);
    // Extract the candidate weights into UserItemCorrelations for sorting.
    final List<UserItemCorrelation> userWeights = Lists.newArrayList();
    for (KijiCell<Double> weight : candidateWeights.<Double>asIterable(productFamily)) {
      userWeights.add(new UserItemCorrelation(
          dataToScore.getEntityId().toString(), weight.getQualifier(), weight.getData()));
    }
    // Sort the candidates by weight.
    Collections.sort(userWeights);
    // Transform the ordered weights to product names.
    final List<String> recommendations =
        Lists.transform(userWeights, new UserItemCorrelationToCorrelatedItemFunction());
    //Remove items which are already in the cart from the recommendations;
    for (String item : recommendations) {
      if (cart.contains(item)) {
        recommendations.remove(item);
      }
    }
    // Return the appropriate number.
    final String numberOfRecommendationsString = context.getParameter(NUMBER_OF_RECS_PARAMETER_KEY);
    final int numberOfRecommendations;
    if (null != numberOfRecommendationsString) {
      numberOfRecommendations = Integer.valueOf(context.getParameter(NUMBER_OF_RECS_PARAMETER_KEY));
    } else {
      numberOfRecommendations = recommendations.size();
    }
    if (numberOfRecommendations < recommendations.size()) {
      return TimestampedValue.create(recommendations.subList(0, numberOfRecommendations));
    } else {
      return TimestampedValue.create(recommendations);
    }
  }
}
