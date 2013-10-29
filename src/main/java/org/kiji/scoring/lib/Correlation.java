package org.kiji.scoring.lib;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.kiji.schema.EntityId;

/** Helper functions and classes for dealing with correlated items. */
public abstract class Correlation {

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

  /** Reusable instance of ItemItemCorrelationToCorrelatedItemFunction. */
  public static final ItemItemCorrelationToCorrelatedItemFunction CORRELATION_TO_ITEM_FUN =
      new ItemItemCorrelationToCorrelatedItemFunction();

  /**
   * Sort a list of ItemItemCorrelations by their correlation weight.
   *
   * @param inputs ItemItemCorrelations to sort.
   * @return Correlations sorted by weight.
   */
  public static List<ItemItemCorrelation> sortByWeight(
      final List<ItemItemCorrelation> inputs
  ) {
    final List<ItemItemCorrelation> outputs = Lists.newArrayList(inputs);
    Collections.sort(outputs);
    return outputs;
  }

  // Below are examples ----------------------------------------------------------------------------

  // A simple workflow:
  // Get a cart of items.
  // For each cart item, get candidates with high correlation from batch processed item data.
  // For each candidate, get the personalized user-item correlation from batch processed user data.
  // Sort the user-item correlations by weight
  // Transform the resulting sorted list using ItemItemCorrelationToCorrelatedItemFunction
  // Return the desired number of recommendations from the head of the resulting list.

  // Get the n best recommendations for a given user and cart.
  // cart
  //   .flatMap(item => candidates)
  //   .map(candidate => personalizedWeight)
  //   .sortBy(personalizedWeight => weight)
  //   .map(personalizedWeight => item)
  //   .take(n)

  /** Get candidate recommendations based on the items in a cart. */
  public abstract List<String> getCandidates(
      final List<String> cart
  );

  /**
   * Get personalized weights for a given user and list of items. The "from" item in the returned
   * correlations is the user name.
   */
  public abstract List<ItemItemCorrelation> getPersonalizedWeights(
      final EntityId user,
      final List<String> candidates
  );

  /** Apply business logic filters to calculated recommendations. */
  public abstract List<String> applyBusinessLogic(
      final List<String> recommendations
  );

  /**
   * Calculate a list of recommendations for a user based on their cart.
   *
   * @param user User for which to calculate recommendations.
   * @param cart User's current cart from which to get candidate items.
   * @return recommended items based on the user's cart.
   */
  public List<String> recommendFromCart(
      final EntityId user,
      final List<String> cart
  ) {
    final List<String> candidates = getCandidates(cart);
    final List<ItemItemCorrelation> weights = getPersonalizedWeights(user, candidates);
    final List<ItemItemCorrelation> sortedWeights = sortByWeight(weights);
    return applyBusinessLogic(Lists.transform(sortedWeights, CORRELATION_TO_ITEM_FUN));
  }
}
