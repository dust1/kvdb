use crate::sql::plan::Node;
use crate::error::Result;
use crate::sql::schema::Catalog;

/// a plan optimizer
pub trait Optimizer {
    fn optimizer(&self, node: Node) -> Result<Node>;
}

/// A constant folding optimizer, which replaces constant expressions with their evaluated value,
/// to prevent it from being re-evaluated over and over again during plan execution.
pub struct ConstantFolder;

/// filter pushdown optimizer, which moves filter predicates into or closer to the source node
pub struct FilterPushdown;

/// An index lookup optimizer, which converts table scans to index lookups
pub struct IndexLookup<'a, C: Catalog> {
    catalog: &'a mut C
}

/// Cleans up noops, e.g. filters with constant true/false predicates.
/// FIXME This should perhaps replace nodes that can never return anything with a Nothing node,
/// but that requires propagating the column names.
pub struct NoopCleaner;

/// Optimizes join types, currently by swapping nested-loop joins with hash joins where appropriate.
pub struct JoinType;
