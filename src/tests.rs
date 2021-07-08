use super::query_builder::*;

// select 1; をクエリビルダで構築する。
#[test]
fn select_constant_value() {
    let query = EmptySelectBuilder::new()
        .select(Value::from(1u64))
        .build()
        .expect("Success building SQL");
    assert_eq!(query.sql(), "SELECT ?;");
    assert_eq!(query.params(), &[Value::Uint(1)]);
}

// select 1 from user as t1; をクエリビルダで構築する。
#[test]
fn select_constant_value_from_user() {
    let builder = EmptySelectBuilder::new();
    let (builder, _) = builder.source("user");
    let query = builder
        .select(Value::from(1u64))
        .build()
        .expect("Success building SQL");
    assert_eq!(query.sql(), "SELECT ? FROM user as t1;");
    assert_eq!(query.params(), &[Value::Uint(1)]);
}

// select 1 from user as a1; をクエリビルダで構築する。
#[test]
fn select_constant_value_from_user_with_alias() {
    let builder = EmptySelectBuilder::new();
    let (mut builder, _) = builder.source("user");
    builder.change_sources_alias_name("a");
    let query = builder
        .select(Value::from(1u64))
        .build()
        .expect("Success building SQL");
    assert_eq!(query.sql(), "SELECT ? FROM user as a1;");
    assert_eq!(query.params(), &[Value::Uint(1)]);
}

// テーブル名のaliasの変更はbuild()までであればどのタイミングでも/何回でも可能。
#[test]
fn changing_sources_alias_can_do_before_call_build() {
    {
        let builder = EmptySelectBuilder::new();
        let (mut builder, _) = builder.source("user");
        builder.change_sources_alias_name("a");
        let query = builder
            .select(Value::from(1u64))
            .build()
            .expect("Success building SQL");
        assert_eq!(query.sql(), "SELECT ? FROM user as a1;");
        assert_eq!(query.params(), &[Value::Uint(1)]);
    }
    {
        let builder = EmptySelectBuilder::new();
        let (mut builder, _) = builder.source("user");
        builder.change_sources_alias_name("a");
        let mut builder = builder.select(Value::from(1u64));
        builder.change_sources_alias_name("b");
        let query = builder.build().expect("Success building SQL");
        assert_eq!(query.sql(), "SELECT ? FROM user as b1;");
        assert_eq!(query.params(), &[Value::Uint(1)]);
    }
    {
        let builder = EmptySelectBuilder::new();
        let (mut builder, _) = builder.source("user");
        builder.change_sources_alias_name("a");
        builder.change_sources_alias_name("b");
        let mut builder = builder.select(Value::from(1u64));
        builder.change_sources_alias_name("c");
        builder.change_sources_alias_name("d");
        let query = builder.build().expect("Success building SQL");
        assert_eq!(query.sql(), "SELECT ? FROM user as d1;");
        assert_eq!(query.params(), &[Value::Uint(1)]);
    }
    {
        let builder = SelectBuilder::new();
        let (mut builder, t1) = builder.source("table1");
        builder.change_sources_alias_name("x");
        let (mut builder, t2) =
            builder.inner_join("table2", |t2| t2.column("id").eq(t1.column("table2_id")));
        builder.change_sources_alias_name("a");
        let query = builder
            .select((t1.column("c1"), t2.column("c1")))
            .build()
            .expect("Success Build SQL");
        assert_eq!(
            query.sql(),
            "SELECT a1.c1, a2.c1 FROM table1 as a1 JOIN table2 as a2 ON a2.id = a1.table2_id;"
        );
        assert_eq!(query.params(), &[]);
    }
}

// select Host, User from user; をクエリビルダで構築する。
#[test]
fn select_columns_from_user_basic() {
    let builder = EmptySelectBuilder::new();
    let (builder, t_user) = builder.source("user");
    let query = builder
        .select((t_user.column("Host"), t_user.column("User")))
        .build()
        .expect("Success build SQL");
    assert_eq!(query.sql(), "SELECT t1.Host, t1.User FROM user as t1;");
    assert_eq!(query.params(), &[]);
}

// select Host, User from user where User = 'root'
// をクエリビルダで構築する。
#[test]
fn select_columns_from_user_with_simple_condition() {
    let builder = SelectBuilder::new();
    let (builder, t_user) = builder.source("user");
    let query = builder
        .filter(t_user.column("User").eq(SqlString::new("root")))
        .select((t_user.column("Host"), t_user.column("User")))
        .build()
        .expect("Success Build SQL");
    assert_eq!(
        query.sql(),
        "SELECT t1.Host, t1.User FROM user as t1 WHERE t1.User = ?;"
    );
    assert_eq!(query.params(), &[Value::String("root".to_string())]);
}

// select t1.c1, t2.c1 from table1 as t1 join table2 as t2 on t2.id = t1.table2_id
// をクエリビルダで構築する。
#[test]
fn select_column_from_two_joined_tables_basic() {
    let builder = SelectBuilder::new();
    let (builder, t1) = builder.source("table1");
    let (builder, t2) =
        builder.inner_join("table2", |t2| t2.column("id").eq(t1.column("table2_id")));
    let query = builder
        .select((t1.column("c1"), t2.column("c1")))
        .build()
        .expect("Success Build SQL");
    assert_eq!(
        query.sql(),
        "SELECT t1.c1, t2.c1 FROM table1 as t1 JOIN table2 as t2 ON t2.id = t1.table2_id;"
    );
    assert_eq!(query.params(), &[]);
}

// select t1.c1, t2.c1, t3.c1 from table1 as t1 join table2 as t2 on t2.id = t1.table2_id left outer join table3 as t3 on t3.id = t2.table3_id where t1.c2 = 1;
// をクエリビルダで構築する。
#[test]
fn select_columns_from_joined_typed_and_untyped_tables_with_condition_1() {
    #[derive(Debug, Clone, Default)]
    struct DB1;
    super::impl_joinable!(DB1);

    #[derive(Debug, Clone, Default)]
    struct DB2;
    super::impl_joinable!(DB2);

    let builder = SelectBuilder::new();
    let table1: TableName<'_, DB1> = "table1".into();
    let table2: TableName<'_, DB1> = "table2".into();
    // let table2: TableName<'_, DB2> = "table2".into(); // compile error
    let (builder, t1) = builder.source(table1);
    let (builder, t2) = builder.inner_join(table2, |t2| t2.column("id").eq(t1.column("table2_id")));
    let (builder, t3) =
        builder.left_outer_join("table3", |t3| t3.column("id").eq(t2.column("table3_id")));
    let query = builder
        .filter(t1.column("c2").eq(SqlInt::new(1)))
        .select((t1.column("c1"), t2.column("c1"), t3.column("c1")))
        .build()
        .expect("Success building SQL");
    assert_eq!(query.sql(), "SELECT t1.c1, t2.c1, t3.c1 FROM table1 as t1 JOIN table2 as t2 ON t2.id = t1.table2_id LEFT OUTER JOIN table3 as t3 ON t3.id = t2.table3_id WHERE t1.c2 = ?;"); // sql:
    assert_eq!(query.params(), &[Value::Int(1)]);
}

// select t1.c1, t2.c1, t3.c1 from table1 as t1 join table2 as t2 on t2.id = t1.table2_id right outer join table3 as t3 on t3.id = t2.table3_id
// をクエリビルダで構築する。
#[test]
fn select_columns_from_joined_tables() {
    let builder = SelectBuilder::new();
    let (builder, t1) = builder.source("table1");
    let (builder, t2) =
        builder.inner_join("table2", |t2| t2.column("id").eq(t1.column("table2_id")));
    let (builder, t3) =
        builder.right_outer_join("table3", |t3| t3.column("id").eq(t2.column("table3_id")));
    let query = builder
        .select((t1.column("c1"), t2.column("c1"), t3.column("c1")))
        .build()
        .expect("Success building SQL");
    assert_eq!(query.sql(), "SELECT t1.c1, t2.c1, t3.c1 FROM table1 as t1 JOIN table2 as t2 ON t2.id = t1.table2_id RIGHT OUTER JOIN table3 as t3 ON t3.id = t2.table3_id;");
    assert_eq!(query.params(), &[]);
}

// select t1.c1, t2.c1 from table1 as t1 cross join table2 as t2
// をクエリビルダで構築する。
#[test]
fn select_columns_from_cross_joined_tables() {
    let builder = SelectBuilder::new();
    let (builder, t1) = builder.source("table1");
    let (builder, t2) = builder.cross_join("table2");
    let query = builder
        .select((t1.column("c1"), t2.column("c1")))
        .build()
        .expect("Success building SQL");
    assert_eq!(
        query.sql(),
        "SELECT t1.c1, t2.c1 FROM table1 as t1 CROSS JOIN table2 as t2;"
    );
    assert_eq!(query.params(), &[]);
}

// select t1.c1 from table1 as t1 where t1.c2 in (1,2,3);
// をクエリビルダで構築する。
#[test]
fn select_column_from_table_with_in_operator() {
    let builder = SelectBuilder::new();
    let (builder, t1) = builder.source("table1");
    let query = builder
        .filter(
            t1.column("c2")
                .any(vec![SqlInt::new(1), SqlInt::new(2), SqlInt::new(3)])
                .and(
                    t1.column("c3")
                        .not_any(vec![SqlInt::new(4), SqlInt::new(5)]),
                ),
        )
        .select(t1.column("c1"))
        .build()
        .expect("Success building SQL");
    assert_eq!(
        query.sql(),
        "SELECT t1.c1 FROM table1 as t1 WHERE t1.c2 IN (?, ?, ?) AND t1.c3 NOT IN (?, ?);"
    );
    assert_eq!(
        query.params(),
        &[
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5)
        ]
    );
}

// select t1.c1 from table1 as t1 where t1.c2 in (1,2,3) and t1.c3 like 'foo%';
// をクエリビルダで構築する。
#[test]
fn select_column_from_table_with_in_and_like_operator() {
    let builder = SelectBuilder::new();
    let (builder, t1) = builder.source("table1");
    let query = builder
        .filter(
            t1.column("c2")
                .any(vec![SqlInt::new(1), SqlInt::new(2), SqlInt::new(3)])
                .and(t1.column("c3").like(SqlString::new("foo%"))),
        )
        .select(t1.column("c1"))
        .build()
        .expect("Success building SQL");
    assert_eq!(
        query.sql(),
        "SELECT t1.c1 FROM table1 as t1 WHERE t1.c2 IN (?, ?, ?) AND t1.c3 LIKE ?;"
    );
    assert_eq!(
        query.params(),
        &[
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::String("foo%".to_string())
        ]
    );
}

// select t1.c1 from table1 as t1 where not ( t1.c2 in (1,2,3) and t1.c3 like 'foo%' and (t1.c4 = t1.c5 or 1 != 5) );
// をクエリビルダで構築する。
#[test]
fn select_column_from_table_with_logical_operators_1() {
    let builder = SelectBuilder::new();
    let (builder, t1) = builder.source("table1");
    let query = builder
        .filter(not(t1
            .column("c2")
            .any(vec![SqlInt::new(1), SqlInt::new(2), SqlInt::new(3)])
            .and(t1.column("c3").like(SqlString::new("foo%")))
            .and(
                t1.column("c4")
                    .eq(t1.column("c5"))
                    .or(SqlInt::new(1).not_eq(SqlInt::new(5))),
            )))
        .select(t1.column("c1"))
        .build()
        .expect("Success building SQL");
    assert_eq!(query.sql(), "SELECT t1.c1 FROM table1 as t1 WHERE NOT (t1.c2 IN (?, ?, ?) AND t1.c3 LIKE ? AND (t1.c4 = t1.c5 OR ? != ?));");
    assert_eq!(
        query.params(),
        &[
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::String("foo%".to_string()),
            Value::Int(1),
            Value::Int(5),
        ]
    );
}

// select t1.c1, t2.c1 from table1 as t1 join table2 as t2 on t2.id = t1.table2_id group by t1.c1, t2.c1 having t1.c1 > 0
// をクエリビルダで構築する。
#[test]
fn select_columns_from_joined_tables_and_grouping_1() {
    let builder = SelectBuilder::new();
    let (builder, t1) = builder.source("table1");
    let (builder, t2) =
        builder.inner_join("table2", |t2| t2.column("id").eq(t1.column("table2_id")));
    let query = builder
        .select((t1.column("c1"), t2.column("c1")))
        .group_by((t1.column("c1"), t2.column("c1")))
        .having(t1.column("c1").gt(SqlInt::new(0)))
        // .order_by(Order::Desc(t1.column("c1")))
        // .limit((10, 15))
        //            .for_update()
        //.lock_in_share_mode()
        .build()
        .expect("Success building SQL");
    assert_eq!(query.sql(), "SELECT t1.c1, t2.c1 FROM table1 as t1 JOIN table2 as t2 ON t2.id = t1.table2_id GROUP BY t1.c1, t2.c1 HAVING t1.c1 > ?;");
    assert_eq!(query.params(), &[Value::Int(0)]);
}

// select DISTINCT t1.c1, t1.c2, t1.c3 from table1 as t1;
// をクエリビルダで実装する。
#[test]
fn select_distinct_1() {
    let builder = SelectBuilder::new();
    let (builder, t1) = builder.source("table1");
    let query = builder
        .select(Distinct::new((
            t1.column("c1"),
            t1.column("c2"),
            t1.column("c3"),
        )))
        .build()
        .expect("Success building SQL");
    assert_eq!(
        query.sql(),
        "SELECT DISTINCT t1.c1, t1.c2, t1.c3 FROM table1 as t1;"
    );
    assert_eq!(query.params(), &[]);
}

// 普通の関数と集約関数の違いとかをテスト
#[test]
fn select_count_from_table_with_condition_1() {
    let builder = SelectBuilder::new();
    let (builder, t1) = builder.source("table1");
    let query = builder
        //            .filter(count(t1.column("c1")).eq(SqlInt::new(0))) // compile error. Cannot use aggregation function in filter().
        .filter(
            date(SqlString::new("2020-02-01 10:11:12"))
                .eq(SqlString::new("2020-02-01"))
                .and(t1.column("c1").between(SqlInt::new(-5), SqlInt::new(5))),
        )
        //            .group_by((count(t1.column("c1")), count(t1.column("c2")))) // compile error. Cannot use aggregation function in group_by().
        .select((
            count(t1.column("c1")),
            left(t1.column("c3"), SqlInt::new(5)),
        ))
        .build()
        .expect("Success building SQL");
    assert_eq!(query.sql(), "SELECT count(t1.c1), left(t1.c3, ?) FROM table1 as t1 WHERE date(?) = ? AND t1.c1 BETWEEN ? AND ?;");
    assert_eq!(
        query.params(),
        &[
            Value::Int(5),
            Value::String("2020-02-01 10:11:12".to_string()),
            Value::String("2020-02-01".to_string()),
            Value::Int(-5),
            Value::Int(5)
        ]
    );
}
