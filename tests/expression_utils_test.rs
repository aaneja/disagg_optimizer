use disagg_optimizer::cascades::expression_utils::{flip_equality, infer_equalities};
use datafusion_expr::Operator;
use datafusion_expr::{BinaryExpr, Expr};

use std::collections::HashSet;
#[test]
fn test_infer_equalities() {
    // Input expressions: a = b, b = c, c = d
    let a = Expr::Column("a".into());
    let b = Expr::Column("b".into());
    let c = Expr::Column("c".into());
    let d = Expr::Column("d".into());
    let e = Expr::Column("e".into());

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
        }),
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(c.clone()),
            op: Operator::Eq,
            right: Box::new(e.clone()),
        }),
    ];

    let mut inferred = HashSet::new();
    inferred.extend(infer_equalities(&equalities));

    // Some of the expected inferred equalities
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
        let flipped = flip_equality(expr);
        assert!(
            inferred.contains(expr) || inferred.contains(&flipped),
            "Missing expected equality: {:?} (or flipped: {:?})",
            expr,
            flipped
        );
    }
}
