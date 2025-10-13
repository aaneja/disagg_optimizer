use datafusion_expr::{BinaryExpr, Expr};
use datafusion_expr_common::operator::Operator;
use std::collections::{HashMap, HashSet};

/// Flips the left and right sides of a BinaryExpr with an `Eq` operator.
/// Returns the original expression if it's not a BinaryExpr with Eq operator.
///
/// # Arguments
/// * `expr` - The expression to flip
///
/// # Returns
/// A new expression with left and right sides flipped if applicable, otherwise the original expression
///
/// # Example
/// ```ignore
/// // Input: a = b
/// // Output: b = a
/// ```
pub fn flip_equality(expr: &Expr) -> Expr {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => Expr::BinaryExpr(BinaryExpr {
            left: right.clone(),
            op: Operator::Eq,
            right: left.clone(),
        }),
        _ => expr.clone(),
    }
}

/// Union-Find (Disjoint Set Union) data structure for tracking equivalence classes.
///
/// This implementation uses path compression and union-by-rank for optimal performance.
struct UnionFind {
    parent: HashMap<Expr, Expr>,
    rank: HashMap<Expr, usize>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            parent: HashMap::new(),
            rank: HashMap::new(),
        }
    }

    /// Finds the root representative of the equivalence class containing `expr`.
    /// Uses path compression to flatten the tree structure for faster future lookups.
    fn find(&mut self, expr: &Expr) -> Expr {
        // If expr is not in the set, it's its own parent
        if !self.parent.contains_key(expr) {
            self.parent.insert(expr.clone(), expr.clone());
            self.rank.insert(expr.clone(), 0);
            return expr.clone();
        }

        let parent = self.parent[expr].clone();

        // Path compression: make expr point directly to root
        if parent != *expr {
            let root = self.find(&parent);
            self.parent.insert(expr.clone(), root.clone());
            root
        } else {
            expr.clone()
        }
    }

    /// Unions the equivalence classes containing `a` and `b`.
    /// Uses union-by-rank to keep the tree balanced.
    fn union(&mut self, a: &Expr, b: &Expr) {
        let root_a = self.find(a);
        let root_b = self.find(b);

        if root_a == root_b {
            return; // Already in the same set
        }

        // Union by rank: attach smaller tree under root of larger tree
        let rank_a = self.rank[&root_a];
        let rank_b = self.rank[&root_b];

        if rank_a < rank_b {
            self.parent.insert(root_a, root_b);
        } else if rank_a > rank_b {
            self.parent.insert(root_b, root_a);
        } else {
            self.parent.insert(root_b, root_a.clone());
            *self.rank.get_mut(&root_a).unwrap() += 1;
        }
    }

    /// Groups all expressions by their root representative.
    fn get_equivalence_classes(&mut self) -> HashMap<Expr, Vec<Expr>> {
        let mut groups: HashMap<Expr, Vec<Expr>> = HashMap::new();

        // Collect all expressions that have been seen
        let all_exprs: Vec<Expr> = self.parent.keys().cloned().collect();

        for expr in all_exprs {
            let root = self.find(&expr);
            groups.entry(root).or_default().push(expr);
        }

        groups
    }
}

pub fn get_unique_equalities(equalities: &[(Expr, Expr)]) -> HashSet<(Expr, Expr)> {
    let mut uf = UnionFind::new();
    for (left, right) in equalities {
        uf.union(left, right);
    }

    let groups = uf.get_equivalence_classes();
    // Get one representative equality from each group
    let mut unique_equalities = HashSet::new();
    for group in groups.values() {
        if group.len() > 1 {
            // Add the first equality in the group as a representative
            unique_equalities.insert((group[0].clone(), group[1].clone()));
        }
    }

    log::debug!("Orig equalities : {:?} Unique Equalities: {:?}", equalities, unique_equalities);
    unique_equalities
}

/// Infers transitive equalities from a list of equality expressions.
///
/// Given a set of equality expressions (e.g., a = b, b = c, c = d),
/// this function returns only the **newly inferred** transitive equalities
/// (e.g., a = c, a = d, b = d) that are not present in the input.
///
/// # Arguments
/// * `equalities` - A vector of equality expressions (Expr::BinaryExpr with Operator::Eq)
///
/// # Returns
/// A vector of inferred equality expressions that are not in the original input
///
/// # Example
/// ```ignore
/// // Input: [a = b, b = c, c = d]
/// // Output: [a = c, a = d, b = d]
/// ```
pub fn infer_equalities(equalities: &Vec<Expr>) -> Vec<Expr> {
    let mut uf = UnionFind::new();

    // Store original equalities to exclude them from results
    let original_equalities: HashSet<Expr> = equalities.iter().cloned().collect();

    // Build the union-find structure from input equalities
    for expr in equalities {
        if let Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) = expr
        {
            uf.union(left, right);
        }
    }

    // Get all equivalence classes
    let groups = uf.get_equivalence_classes();

    // Generate all pairwise equalities within each equivalence class
    let mut all_equalities = Vec::new();
    for group in groups.values() {
        // Skip singleton groups (no equalities to infer)
        if group.len() < 2 {
            continue;
        }

        // Generate all pairs within the group
        for i in 0..group.len() {
            for j in i + 1..group.len() {
                let equality = Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(group[i].clone()),
                    op: Operator::Eq,
                    right: Box::new(group[j].clone()),
                });

                // Only include if it's not in the original input
                if !original_equalities.contains(&equality) {
                    all_equalities.push(equality);
                }
            }
        }
    }

    all_equalities
}
