use crate::cascades::expression_utils::infer_equalities;
use datafusion_expr::{BinaryExpr, Expr};
use datafusion_expr::Operator;

#[cfg(test)]
mod tests {

    use std::collections::{HashSet};

    use super::*;
    #[test]
    fn test_infer_equalities() {
        // Input expressions: a = b, b = c, c = d, e = f
        let a = Expr::Column("a".into());
        let b = Expr::Column("b".into());
        let c = Expr::Column("c".into());
        let d = Expr::Column("d".into());

        let equalities = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(a.clone()),
                op: Operator::Eq,
                right: Box::new(b.clone()),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(b.clone()),
                op: Operator::Eq,
                right: Box::new(c.clone()),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(c.clone()),
                op: Operator::Eq,
                right: Box::new(d.clone()),
            })
        ];

        let mut inferred = HashSet::new();
        inferred.extend(infer_equalities(&equalities));

        // Expected inferred equalities
        let expected = vec![
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(a.clone()),
                op: Operator::Eq,
                right: Box::new(c.clone()),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(a.clone()),
                op: Operator::Eq,
                right: Box::new(d.clone()),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(b.clone()),
                op: Operator::Eq,
                right: Box::new(d.clone()),
            }),
        ];

        println!("Inferred Equalities: {:?}", inferred);

        for expr in &expected {
            assert!(inferred.contains(expr), "Missing expected equality: {:?}", expr);
        }

        // Ensure no extra equalities are inferred
        assert_eq!(inferred.len(), expected.len());
    }
}