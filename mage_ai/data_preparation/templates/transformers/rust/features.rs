use mage::prelude::*;

/// Features for a model, per entity and in time order: the previous value, the change
/// since it, a running mean, a z-score within the entity, and the value's rank. Change
/// `entity`, `order` and `value` to your columns.
fn transform(data: LazyFrame) -> Result<LazyFrame> {
    let entity = "customer_id";
    let order = "id";
    let value = "amount";
    let per_entity = |expression: Expr| expression.over([col(entity)]);

    let previous = per_entity(col(value).shift(lit(1)))?;
    let running_sum = per_entity(col(value).cum_sum(false))?;
    let running_count = per_entity(col(value).cum_count(false))?;
    let mean = per_entity(col(value).mean())?;
    let std = per_entity(col(value).std(1))?;
    let rank = per_entity(col(value).rank(RankOptions::default(), None))?;

    Ok(data
        .sort([entity, order], SortMultipleOptions::default())
        .with_columns([
            previous.clone().alias("previous"),
            (col(value) - previous).alias("change"),
            (running_sum / running_count.cast(DataType::Float64)).alias("running_mean"),
            ((col(value) - mean) / std).alias("zscore"),
            rank.alias("rank_in_entity"),
        ]))
}

fn test_the_features_are_there(output: &DataFrame) -> Result<()> {
    for name in ["previous", "change", "running_mean", "zscore", "rank_in_entity"] {
        ensure!(output.column(name).is_ok(), "The feature {name} is missing");
    }
    Ok(())
}
