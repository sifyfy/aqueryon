//! Query Builder
//!

pub mod synonym {
    use crate::query_builder as qb;

    pub type EmptySelectBuilder = qb::SelectBuilder<
        qb::EmptyFromClause,
        qb::EmptyWhereClause,
        (),
        qb::EmptyGroupByClause,
        qb::EmptyHavingClause,
        qb::EmptyOrderByClause,
        qb::EmptyLimitClause,
        qb::LockModeDefaultBehavior,
    >;

    pub type SourceUpdatedBuilder<QS> = qb::SelectBuilder<
        qb::FromClause<QS>,
        qb::EmptyWhereClause,
        (),
        qb::EmptyGroupByClause,
        qb::EmptyHavingClause,
        qb::EmptyOrderByClause,
        qb::EmptyLimitClause,
        qb::LockModeDefaultBehavior,
    >;

    pub type Join<L, R, EXP> = qb::Join<L, IntoQuerySourceRef<R>, EXP>;

    pub type LeftOuterJoin<L, R, EXP> = qb::Join<L, IntoNullableQuerySourceRef<R>, EXP>;

    pub type RightOuterJoin<L, R, EXP> =
        qb::Join<<L as qb::QuerySource>::NullableSelf, IntoQuerySourceRef<R>, EXP>;

    pub type CrossJoin<L, R> = qb::Join<L, IntoQuerySourceRef<R>, qb::BlankBoolExpression>;

    pub type IntoQuerySource<QS> = <QS as qb::IntoQuerySource>::QuerySource;

    pub type IntoNullableQuerySource<QS> =
        <<QS as qb::IntoQuerySource>::QuerySource as qb::QuerySource>::NullableSelf;

    pub type IntoQuerySourceRef<QS> = qb::QuerySourceRef<IntoQuerySource<QS>>;

    pub type IntoNullableQuerySourceRef<QS> = qb::QuerySourceRef<IntoNullableQuerySource<QS>>;
}

use std::borrow::Borrow;
use std::cell::RefCell;
use std::convert::TryInto;
use std::io::Write;
use std::marker::PhantomData;
use std::rc::Rc;
use std::string::FromUtf8Error;
pub use synonym::EmptySelectBuilder;

macro_rules! define_select_clause {
    ( $type_name:ident, $empty_type:tt, $clause:expr ) => {
        #[derive(Debug, Clone, Default)]
        pub struct $empty_type;

        #[derive(Debug, Clone)]
        pub struct $type_name<T>(T);

        impl $empty_type {
            pub fn new() -> Self {
                $empty_type
            }
        }

        impl<T> $type_name<T> {
            pub fn new(source: T) -> Self {
                $type_name(source)
            }

            pub fn unwrap(self) -> T {
                self.0
            }

            pub fn inner_ref(&self) -> &T {
                &self.0
            }
        }

        impl BuildSql for $empty_type {
            fn build_sql(
                &self,
                _buf: &mut Vec<u8>,
                _params: &mut Vec<Value>,
            ) -> Result<(), BuildSqlError> {
                Ok(())
            }
        }

        impl<T> BuildSql for $type_name<T>
        where
            T: BuildSql,
        {
            fn build_sql(
                &self,
                buf: &mut Vec<u8>,
                params: &mut Vec<Value>,
            ) -> Result<(), BuildSqlError> {
                write!(buf, $clause)?;
                self.inner_ref().build_sql(buf, params)
            }
        }
    };
}

define_select_clause!(FromClause, EmptyFromClause, " FROM ");
define_select_clause!(WhereClause, EmptyWhereClause, " WHERE ");
define_select_clause!(GroupByClause, EmptyGroupByClause, " GROUP BY ");
define_select_clause!(HavingClause, EmptyHavingClause, " HAVING ");
define_select_clause!(OrderByClause, EmptyOrderByClause, " ORDER BY ");
define_select_clause!(LimitClause, EmptyLimitClause, " LIMIT ");

#[derive(Debug, Clone)]
pub struct SelectBuilder<QS, W, C, G, H, O, L, LM> {
    sources: QS,
    sources_num: u8,
    sources_alias_name: SourceAliasName,
    filter: W,
    columns: C,
    group_by: G,
    having: H,
    order_by: O,
    limit: L,
    lock_mode: LM,
}

impl<QS, W, C, G, H, O, L, LM> SelectBuilder<QS, W, C, G, H, O, L, LM> {
    pub fn change_sources_alias_name(&mut self, new_name: &'static str) {
        self.sources_alias_name.set(new_name)
    }
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
pub struct LockModeDefaultBehavior;

impl BuildSql for LockModeDefaultBehavior {
    fn build_sql(&self, _buf: &mut Vec<u8>, _params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
pub struct ForUpdate;

impl BuildSql for ForUpdate {
    fn build_sql(&self, buf: &mut Vec<u8>, _params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        Ok(write!(buf, " FOR UPDATE")?)
    }
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
pub struct LockInShareMode;

impl BuildSql for LockInShareMode {
    fn build_sql(&self, buf: &mut Vec<u8>, _params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        Ok(write!(buf, " LOCK IN SHARE MODE")?)
    }
}

impl Default for synonym::EmptySelectBuilder {
    fn default() -> Self {
        SelectBuilder {
            sources: EmptyFromClause,
            sources_num: 0,
            sources_alias_name: SourceAliasName::default(),
            filter: EmptyWhereClause,
            columns: (),
            group_by: EmptyGroupByClause,
            having: EmptyHavingClause,
            order_by: EmptyOrderByClause,
            limit: EmptyLimitClause,
            lock_mode: LockModeDefaultBehavior,
        }
    }
}

impl synonym::EmptySelectBuilder {
    pub fn new() -> synonym::EmptySelectBuilder {
        Default::default()
    }

    pub fn source<QS>(
        self,
        source: QS,
    ) -> (
        synonym::SourceUpdatedBuilder<synonym::IntoQuerySourceRef<QS>>,
        synonym::IntoQuerySourceRef<QS>,
    )
    where
        QS: IntoQuerySource,
        QS::QuerySource: QuerySource + Clone,
    {
        let sources_num = self.sources_num + 1;
        let src_ref = QuerySourceRef::new(
            source.into_query_source(),
            SourceAlias::new(self.sources_alias_name.clone(), sources_num),
        );
        let ret_src_ref = src_ref.clone();
        let new_builder = SelectBuilder {
            sources: FromClause::new(src_ref),
            sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: (),
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            lock_mode: self.lock_mode,
        };
        (new_builder, ret_src_ref)
    }
}

impl<QS, W, G, H, O, L, LM> SelectBuilder<QS, W, (), G, H, O, L, LM> {
    pub fn select<C>(self, columns: C) -> SelectBuilder<QS, W, C, G, H, O, L, LM>
    where
        C: Columns,
    {
        SelectBuilder {
            sources: self.sources,
            sources_num: self.sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            lock_mode: self.lock_mode,
        }
    }
}

impl<QS>
    SelectBuilder<
        FromClause<QS>,
        EmptyWhereClause,
        (),
        EmptyGroupByClause,
        EmptyHavingClause,
        EmptyOrderByClause,
        EmptyLimitClause,
        LockModeDefaultBehavior,
    >
where
    QS: QuerySource,
{
    pub fn inner_join<QS2, ON, EXP>(
        self,
        source: QS2,
        mut on: ON,
    ) -> (
        synonym::SourceUpdatedBuilder<synonym::Join<QS, QS2, EXP>>,
        synonym::IntoQuerySourceRef<QS2>,
    )
    where
        QS2: IntoQuerySource,
        QS2::Database: Joinable<QS::Database>,
        QS2::QuerySource: QuerySource + Clone,
        ON: FnMut(QuerySourceRef<QS2::QuerySource>) -> EXP,
        EXP: Expression<SqlType = SqlTypeBool>,
    {
        let sources_num = self.sources_num + 1;
        let src_ref = QuerySourceRef::new(
            source.into_query_source(),
            SourceAlias::new(self.sources_alias_name.clone(), sources_num),
        );
        let ret_src_ref = src_ref.clone();
        let on_expr = on(src_ref.clone());
        let new_builder = SelectBuilder {
            sources: FromClause::new(Join::Inner(self.sources.unwrap(), src_ref, on_expr)),
            sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: (),
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            lock_mode: self.lock_mode,
        };
        (new_builder, ret_src_ref)
    }

    pub fn left_outer_join<QS2, ON, EXP>(
        self,
        source: QS2,
        mut on: ON,
    ) -> (
        synonym::SourceUpdatedBuilder<synonym::LeftOuterJoin<QS, QS2, EXP>>,
        synonym::IntoNullableQuerySourceRef<QS2>,
    )
    where
        QS2: IntoQuerySource,
        QS2::Database: Joinable<QS::Database>,
        QS2::QuerySource: QuerySource + Clone,
        <QS2::QuerySource as QuerySource>::NullableSelf: Clone,
        ON: FnMut(QuerySourceRef<<QS2::QuerySource as QuerySource>::NullableSelf>) -> EXP,
        EXP: Expression<SqlType = SqlTypeBool>,
    {
        let sources_num = self.sources_num + 1;
        let src_ref = QuerySourceRef::new(
            source.into_query_source().nullable(),
            SourceAlias::new(self.sources_alias_name.clone(), sources_num),
        );
        let ret_src_ref = src_ref.clone();
        let on_expr = on(src_ref.clone());
        let new_builder = SelectBuilder {
            sources: FromClause::new(Join::LeftOuter(self.sources.unwrap(), src_ref, on_expr)),
            sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: (),
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            lock_mode: self.lock_mode,
        };
        (new_builder, ret_src_ref)
    }

    pub fn right_outer_join<QS2, ON, EXP>(
        self,
        source: QS2,
        mut on: ON,
    ) -> (
        synonym::SourceUpdatedBuilder<synonym::RightOuterJoin<QS, QS2, EXP>>,
        synonym::IntoQuerySourceRef<QS2>,
    )
    where
        QS2: IntoQuerySource,
        QS2::Database: Joinable<QS::Database>,
        QS2::QuerySource: QuerySource + Clone,
        ON: FnMut(QuerySourceRef<QS2::QuerySource>) -> EXP,
        EXP: Expression<SqlType = SqlTypeBool>,
    {
        let sources_num = self.sources_num + 1;
        let src_ref = QuerySourceRef::new(
            source.into_query_source(),
            SourceAlias::new(self.sources_alias_name.clone(), sources_num),
        );
        let ret_src_ref = src_ref.clone();
        let on_expr = on(src_ref.clone());
        let new_builder = SelectBuilder {
            sources: FromClause::new(Join::RightOuter(
                self.sources.unwrap().nullable(),
                src_ref,
                on_expr,
            )),
            sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: (),
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            lock_mode: self.lock_mode,
        };
        (new_builder, ret_src_ref)
    }

    pub fn cross_join<QS2>(
        self,
        source: QS2,
    ) -> (
        synonym::SourceUpdatedBuilder<synonym::CrossJoin<QS, QS2>>,
        synonym::IntoQuerySourceRef<QS2>,
    )
    where
        QS2: IntoQuerySource,
        QS2::Database: Joinable<QS::Database>,
        QS2::QuerySource: QuerySource + Clone,
    {
        let sources_num = self.sources_num + 1;
        let src_ref = QuerySourceRef::new(
            source.into_query_source(),
            SourceAlias::new(self.sources_alias_name.clone(), sources_num),
        );
        let ret_src_ref = src_ref.clone();
        let new_builder = SelectBuilder {
            sources: FromClause::new(Join::Cross(self.sources.unwrap(), src_ref)),
            sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: (),
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            lock_mode: self.lock_mode,
        };
        (new_builder, ret_src_ref)
    }

    pub fn filter<W>(
        self,
        expr: W,
    ) -> SelectBuilder<
        FromClause<QS>,
        WhereClause<W>,
        (),
        EmptyGroupByClause,
        EmptyHavingClause,
        EmptyOrderByClause,
        EmptyLimitClause,
        LockModeDefaultBehavior,
    >
    where
        W: Expression<SqlType = SqlTypeBool, Aggregation = NonAggregate>,
    {
        SelectBuilder {
            sources: self.sources,
            sources_num: self.sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: WhereClause::new(expr),
            columns: (),
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            lock_mode: self.lock_mode,
        }
    }
}

impl<QS, W, C>
    SelectBuilder<
        FromClause<QS>,
        W,
        C,
        EmptyGroupByClause,
        EmptyHavingClause,
        EmptyOrderByClause,
        EmptyLimitClause,
        LockModeDefaultBehavior,
    >
where
    QS: QuerySource,
{
    pub fn group_by<G>(
        self,
        group: G,
    ) -> SelectBuilder<
        FromClause<QS>,
        W,
        C,
        GroupByClause<G>,
        EmptyHavingClause,
        EmptyOrderByClause,
        EmptyLimitClause,
        LockModeDefaultBehavior,
    >
    where
        G: Columns<Aggregation = NonAggregate>,
    {
        SelectBuilder {
            sources: self.sources,
            sources_num: self.sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: self.columns,
            group_by: GroupByClause::new(group),
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            lock_mode: self.lock_mode,
        }
    }
}

impl<QS, W, C, G>
    SelectBuilder<
        FromClause<QS>,
        W,
        C,
        GroupByClause<G>,
        EmptyHavingClause,
        EmptyOrderByClause,
        EmptyLimitClause,
        LockModeDefaultBehavior,
    >
where
    QS: QuerySource,
    G: Columns,
{
    pub fn having<H>(
        self,
        having: H,
    ) -> SelectBuilder<
        FromClause<QS>,
        W,
        C,
        GroupByClause<G>,
        HavingClause<H>,
        EmptyOrderByClause,
        EmptyLimitClause,
        LockModeDefaultBehavior,
    >
    where
        H: Expression<SqlType = SqlTypeBool>,
    {
        SelectBuilder {
            sources: self.sources,
            sources_num: self.sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: self.columns,
            group_by: self.group_by,
            having: HavingClause::new(having),
            order_by: self.order_by,
            limit: self.limit,
            lock_mode: self.lock_mode,
        }
    }
}

impl<QS, W, C, G, H>
    SelectBuilder<
        FromClause<QS>,
        W,
        C,
        G,
        H,
        EmptyOrderByClause,
        EmptyLimitClause,
        LockModeDefaultBehavior,
    >
where
    QS: QuerySource,
{
    pub fn order_by<O>(
        self,
        order: O,
    ) -> SelectBuilder<
        FromClause<QS>,
        W,
        C,
        G,
        H,
        OrderByClause<O>,
        EmptyLimitClause,
        LockModeDefaultBehavior,
    >
    where
        O: Orders,
    {
        SelectBuilder {
            sources: self.sources,
            sources_num: self.sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: self.columns,
            group_by: self.group_by,
            having: self.having,
            order_by: OrderByClause::new(order),
            limit: self.limit,
            lock_mode: self.lock_mode,
        }
    }
}

impl<QS, W, C, G, H, O>
    SelectBuilder<FromClause<QS>, W, C, G, H, O, EmptyLimitClause, LockModeDefaultBehavior>
where
    QS: QuerySource,
{
    pub fn limit<L>(
        self,
        limit: L,
    ) -> SelectBuilder<FromClause<QS>, W, C, G, H, O, LimitClause<Limit>, LockModeDefaultBehavior>
    where
        L: Into<Limit>,
    {
        SelectBuilder {
            sources: self.sources,
            sources_num: self.sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: self.columns,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: LimitClause::new(limit.into()),
            lock_mode: self.lock_mode,
        }
    }
}

impl<QS, W, C, G, H, O, L> SelectBuilder<FromClause<QS>, W, C, G, H, O, L, LockModeDefaultBehavior>
where
    QS: QuerySource,
{
    pub fn for_update(self) -> SelectBuilder<FromClause<QS>, W, C, G, H, O, L, ForUpdate> {
        self.set_lock_mode(ForUpdate)
    }

    pub fn lock_in_share_mode(
        self,
    ) -> SelectBuilder<FromClause<QS>, W, C, G, H, O, L, LockInShareMode> {
        self.set_lock_mode(LockInShareMode)
    }

    fn set_lock_mode<LM>(
        self,
        lock_mode: LM,
    ) -> SelectBuilder<FromClause<QS>, W, C, G, H, O, L, LM> {
        SelectBuilder {
            sources: self.sources,
            sources_num: self.sources_num,
            sources_alias_name: self.sources_alias_name,
            filter: self.filter,
            columns: self.columns,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            limit: self.limit,
            lock_mode,
        }
    }
}

impl<QS, C, W, G, H, O, L, LM> SelectBuilder<QS, W, C, G, H, O, L, LM>
where
    QS: BuildSql,
    C: BuildSql,
    W: BuildSql,
    G: BuildSql,
    H: BuildSql,
    O: BuildSql,
    L: BuildSql,
    LM: BuildSql,
{
    pub fn build(self) -> Result<Query, QueryBuildError> {
        Query::build(|buf, params| {
            write!(buf, "SELECT ")?;
            self.columns.build_sql(buf, params)?;
            self.sources.build_sql(buf, params)?;
            self.filter.build_sql(buf, params)?;
            self.group_by.build_sql(buf, params)?;
            self.having.build_sql(buf, params)?;
            self.order_by.build_sql(buf, params)?;
            self.limit.build_sql(buf, params)?;
            self.lock_mode.build_sql(buf, params)?;
            write!(buf, ";")?;
            Ok(())
        })
    }
}

impl<QS, C, W, G, H, O, L, LM> BuildSql for SelectBuilder<QS, W, C, G, H, O, L, LM>
where
    QS: BuildSql,
    C: BuildSql,
    W: BuildSql,
    G: BuildSql,
    H: BuildSql,
    O: BuildSql,
    L: BuildSql,
    LM: BuildSql,
{
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        write!(buf, "(SELECT ")?;
        self.columns.build_sql(buf, params)?;
        self.sources.build_sql(buf, params)?;
        self.filter.build_sql(buf, params)?;
        self.group_by.build_sql(buf, params)?;
        self.having.build_sql(buf, params)?;
        self.order_by.build_sql(buf, params)?;
        self.limit.build_sql(buf, params)?;
        self.lock_mode.build_sql(buf, params)?;
        write!(buf, ")")?;
        Ok(())
    }
}

impl<QS, W, C, G, H, O, L, LM> Expression for SelectBuilder<QS, W, C, G, H, O, L, LM>
where
    C: Columns,
{
    type SqlType = C::SqlType;
    type Term = Monomial;
    type BoolOperation = NonBool;
    type Aggregation = NonAggregate; // サブクエリなので必ず値になる
}

#[derive(Debug, Clone)]
pub struct Query {
    sql: String,
    params: Vec<Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum QueryBuildError {
    #[error("QueryBuildError::BuildSqlError: {0}")]
    BuildSqlError(#[from] BuildSqlError),
    #[error("QueryBuildError::EncodeError: {0}")]
    EncodeError(#[from] FromUtf8Error),
}

impl Query {
    pub fn build<F>(mut f: F) -> Result<Query, QueryBuildError>
    where
        F: FnMut(&mut Vec<u8>, &mut Vec<Value>) -> Result<(), BuildSqlError>,
    {
        let num_of_params = 32;
        let mut buf: Vec<u8> = Vec::with_capacity(128);
        let mut params: Vec<Value> = Vec::with_capacity(num_of_params); // TODO: メモリの再アロケートを繰り返さない為にパラメータの数だけ最初から確保したほうがいい

        f(&mut buf, &mut params)?;

        Ok(Query {
            sql: String::from_utf8(buf)?,
            params,
        })
    }

    pub fn sql(&self) -> &str {
        self.sql.as_str()
    }

    pub fn params(&self) -> &[Value] {
        &self.params
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum Value {
    Null,
    String(String),
    Int(i64),
    Uint(u64),
}

impl Expression for Value {
    type SqlType = SqlTypeAny;
    type Term = Monomial;
    type BoolOperation = BoolMono;
    type Aggregation = NonAggregate;
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::Uint(value)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(value: Option<T>) -> Self {
        value.map(Into::into).unwrap_or(Value::Null)
    }
}

impl From<SqlString> for Value {
    fn from(value: SqlString) -> Self {
        Value::String(value.0)
    }
}

impl From<SqlInt> for Value {
    fn from(value: SqlInt) -> Self {
        Value::Int(value.0)
    }
}

impl From<SqlUint> for Value {
    fn from(value: SqlUint) -> Self {
        Value::Uint(value.0)
    }
}

impl<T> BuildSql for T
where
    T: Into<Value> + Clone,
{
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        write!(buf, "?")?;
        params.push((*self).clone().into());
        Ok(())
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SqlString(String);

impl SqlString {
    pub fn new(s: &str) -> SqlString {
        SqlString(s.to_string())
    }
}

impl<T> From<T> for SqlString
where
    T: AsRef<str>,
{
    fn from(value: T) -> Self {
        SqlString::new(value.as_ref())
    }
}

impl Expression for SqlString {
    type SqlType = SqlTypeString;
    type Term = Monomial;
    type BoolOperation = NonBool;
    type Aggregation = NonAggregate;
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SqlInt(i64);

impl SqlInt {
    pub fn new(i: i64) -> SqlInt {
        SqlInt(i)
    }

    pub fn try_from<T: TryInto<i64>>(value: T) -> Result<SqlInt, T::Error> {
        Ok(SqlInt::new(value.try_into()?))
    }
}

impl<T> From<T> for SqlInt
where
    T: Into<i64>,
{
    fn from(value: T) -> Self {
        SqlInt(value.into())
    }
}

impl Expression for SqlInt {
    type SqlType = SqlTypeInt;
    type Term = Monomial;
    type BoolOperation = NonBool;
    type Aggregation = NonAggregate;
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SqlUint(u64);

impl SqlUint {
    pub fn new(i: u64) -> SqlUint {
        SqlUint(i)
    }

    pub fn try_from<T: TryInto<u64>>(value: T) -> Result<SqlUint, T::Error> {
        Ok(SqlUint::new(value.try_into()?))
    }
}

impl<T> From<T> for SqlUint
where
    T: Into<u64>,
{
    fn from(value: T) -> Self {
        SqlUint(value.into())
    }
}

impl Expression for SqlUint {
    type SqlType = SqlTypeUint;
    type Term = Monomial;
    type BoolOperation = NonBool;
    type Aggregation = NonAggregate;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Hash)]
pub struct Dual;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Hash)]
pub struct AnyDatabase;

pub trait QuerySource {
    type Database;
    type NullableSelf: QuerySource<Database = Self::Database>;

    fn nullable(self) -> Self::NullableSelf;
}

pub trait IntoQuerySource {
    type Database;
    type QuerySource: QuerySource<Database = Self::Database>;

    fn into_query_source(self) -> Self::QuerySource;
}

impl<'a> IntoQuerySource for &'a str {
    type Database = AnyDatabase;
    type QuerySource = TableName<'a, AnyDatabase>;

    fn into_query_source(self) -> Self::QuerySource {
        TableName::new(self)
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Default, Hash, derive_more::Deref)]
pub struct TableName<'a, DB> {
    #[deref]
    name: &'a str,
    db: PhantomData<DB>,
}

impl<DB> TableName<'_, DB> {
    pub fn new(name: &str) -> TableName<'_, DB> {
        TableName {
            name,
            db: PhantomData,
        }
    }
}

impl<'a, DB> From<&'a str> for TableName<'a, DB> {
    fn from(value: &'a str) -> Self {
        TableName::new(value)
    }
}

pub trait Joinable<DB> {
    type Database;
}

impl Joinable<AnyDatabase> for AnyDatabase {
    type Database = AnyDatabase;
}

#[macro_export]
macro_rules! impl_joinable {
    ($ty:ty) => {
        impl $crate::query_builder::Joinable<$ty> for $ty {
            type Database = $ty;
        }

        impl $crate::query_builder::Joinable<$crate::query_builder::AnyDatabase> for $ty {
            type Database = $ty;
        }

        impl $crate::query_builder::Joinable<$ty> for $crate::query_builder::AnyDatabase {
            type Database = $ty;
        }
    };
}

// 個別のテーブル型ではNullableSelfにOption<カラムの型>を返すメソッドを生やす。
// left_outer_join等が呼ばれたらbuilderがnullableメソッドをコールする。
impl<DB> QuerySource for TableName<'_, DB> {
    type Database = DB;
    type NullableSelf = Self;

    fn nullable(self) -> Self::NullableSelf {
        self
    }
}

impl<DB> IntoQuerySource for TableName<'_, DB>
where
    DB: Clone,
{
    type Database = DB;
    type QuerySource = Self;

    fn into_query_source(self) -> Self::QuerySource {
        self
    }
}

impl<DB> BuildSql for TableName<'_, DB> {
    fn build_sql(&self, buf: &mut Vec<u8>, _params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        write!(buf, "{}", self.name)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Column<ST = SqlTypeAny> {
    table_name: SourceAlias,
    column_name: String,
    sql_type: ST,
}

impl<ST> Column<ST>
where
    ST: Default,
{
    pub fn new<T>(table_name: SourceAlias, column_name: T) -> Column<ST>
    where
        T: AsColumnName + Sized,
    {
        Column {
            table_name,
            column_name: column_name.as_column_name().to_string(),
            sql_type: Default::default(),
        }
    }
}

impl<ST> Expression for Column<ST> {
    type SqlType = ST;
    type Term = Monomial;
    type BoolOperation = NonBool;
    type Aggregation = NonAggregate;
}

#[derive(Debug, Clone)]
pub struct SourceAliasName {
    name: Rc<RefCell<&'static str>>,
}

impl SourceAliasName {
    pub fn new(name: &'static str) -> Self {
        SourceAliasName {
            name: Rc::new(RefCell::new(name)),
        }
    }

    pub fn set(&mut self, new_name: &'static str) {
        self.name.replace(new_name);
    }

    pub fn as_str(&self) -> &'static str {
        RefCell::borrow(&self.name).borrow()
    }
}

impl Default for SourceAliasName {
    fn default() -> Self {
        Self::new("t")
    }
}

#[derive(Clone)]
pub struct SourceAlias {
    name: SourceAliasName,
    suffix_number: u8,
}

impl SourceAlias {
    pub fn new(name: SourceAliasName, suffix_number: u8) -> Self {
        SourceAlias {
            name,
            suffix_number,
        }
    }

    pub fn change_name(&mut self, new_name: &'static str) {
        self.name.set(new_name);
    }
}

impl ToString for SourceAlias {
    fn to_string(&self) -> String {
        format!("{}{}", self.name.as_str(), self.suffix_number)
    }
}

#[derive(Clone, derive_more::Deref)]
pub struct QuerySourceRef<QS> {
    #[deref]
    source: QS,
    alias: SourceAlias,
}

impl<QS> QuerySourceRef<QS>
where
    QS: QuerySource,
{
    pub fn new(source: QS, alias: SourceAlias) -> QuerySourceRef<QS> {
        QuerySourceRef { source, alias }
    }

    pub fn column<T>(&self, column_name: T) -> Column
    where
        T: AsColumnName + Sized,
    {
        Column::new(self.alias.clone(), column_name)
    }

    pub fn typed_column<T: Default>(&self, column_name: impl AsColumnName + Sized) -> Column<T> {
        Column::new(self.alias.clone(), column_name)
    }

    pub fn alias(&self) -> String {
        self.alias.to_string()
    }
}

impl<QS> QuerySource for QuerySourceRef<QS>
where
    QS: QuerySource,
{
    type Database = QS::Database;
    type NullableSelf = QuerySourceRef<QS::NullableSelf>;

    fn nullable(self) -> Self::NullableSelf {
        QuerySourceRef {
            source: self.source.nullable(),
            alias: self.alias.clone(),
        }
    }
}

impl<QS> BuildSql for QuerySourceRef<QS>
where
    QS: QuerySource + BuildSql,
{
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        self.source
            .build_sql(buf, params)
            .map_err(anyhow::Error::from)?;
        write!(buf, " as {}", self.alias.to_string()).map_err(anyhow::Error::from)?;
        Ok(())
    }
}

pub trait AsColumnName {
    fn as_column_name(&self) -> &str;
}

impl AsColumnName for &str {
    fn as_column_name(&self) -> &str {
        self
    }
}

impl<T> AsColumnName for &T
where
    T: AsColumnName,
{
    fn as_column_name(&self) -> &str {
        (**self).as_column_name()
    }
}

impl<T> AsColumnName for &mut T
where
    T: AsColumnName,
{
    fn as_column_name(&self) -> &str {
        (**self).as_column_name()
    }
}

#[derive(Debug, Clone)]
pub enum Join<L, R, ON> {
    Inner(L, R, ON),
    LeftOuter(L, R, ON),
    RightOuter(L, R, ON),
    Cross(L, R),
}

impl<L, R, E> QuerySource for Join<L, R, E>
where
    L: QuerySource,
    R: QuerySource,
    L::Database: Joinable<R::Database>,
    L::NullableSelf: QuerySource,
    R::NullableSelf: QuerySource,
    E: Expression<SqlType = SqlTypeBool> + Clone,
{
    type Database = <L::Database as Joinable<R::Database>>::Database;
    type NullableSelf = Join<L::NullableSelf, R::NullableSelf, E>;

    fn nullable(self) -> Self::NullableSelf {
        match self {
            Join::Inner(l, r, on) => Join::Inner(l.nullable(), r.nullable(), on),
            Join::LeftOuter(l, r, on) => Join::LeftOuter(l.nullable(), r.nullable(), on),
            Join::RightOuter(l, r, on) => Join::RightOuter(l.nullable(), r.nullable(), on),
            Join::Cross(l, r) => Join::Cross(l.nullable(), r.nullable()),
        }
    }
}

impl<L, R, E> BuildSql for Join<L, R, E>
where
    L: QuerySource + BuildSql,
    R: QuerySource + BuildSql,
    E: Expression<SqlType = SqlTypeBool> + BuildSql,
{
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        match self {
            Join::Inner(l, r, on) => (|| -> Result<(), anyhow::Error> {
                l.build_sql(buf, params)?;
                write!(buf, " JOIN ")?;
                r.build_sql(buf, params)?;
                write!(buf, " ON ")?;
                on.build_sql(buf, params)?;
                Ok(())
            })()
            .map_err(From::from),
            Join::LeftOuter(l, r, on) => (|| -> Result<(), anyhow::Error> {
                l.build_sql(buf, params)?;
                write!(buf, " LEFT OUTER JOIN ")?;
                r.build_sql(buf, params)?;
                write!(buf, " ON ")?;
                on.build_sql(buf, params)?;
                Ok(())
            })()
            .map_err(From::from),
            Join::RightOuter(l, r, on) => (|| -> Result<(), anyhow::Error> {
                l.build_sql(buf, params)?;
                write!(buf, " RIGHT OUTER JOIN ")?;
                r.build_sql(buf, params)?;
                write!(buf, " ON ")?;
                on.build_sql(buf, params)?;
                Ok(())
            })()
            .map_err(From::from),
            Join::Cross(l, r) => (|| -> Result<(), anyhow::Error> {
                l.build_sql(buf, params)?;
                write!(buf, " CROSS JOIN ")?;
                r.build_sql(buf, params)?;
                Ok(())
            })()
            .map_err(From::from),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Monomial;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Polynomial;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Hash)]
pub struct BoolAnd;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Hash)]
pub struct BoolOr;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Hash)]
pub struct BoolMono;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Hash)]
pub struct NonBool;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Hash)]
pub struct Aggregate;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Hash)]
pub struct NonAggregate;

pub trait Aggregation<T> {
    type Output;
}

impl Aggregation<Aggregate> for NonAggregate {
    type Output = Aggregate;
}

impl Aggregation<NonAggregate> for Aggregate {
    type Output = Aggregate;
}

impl Aggregation<Aggregate> for Aggregate {
    type Output = Aggregate;
}

impl Aggregation<NonAggregate> for NonAggregate {
    type Output = NonAggregate;
}

impl Aggregation<()> for Aggregate {
    type Output = Aggregate;
}

impl Aggregation<()> for NonAggregate {
    type Output = NonAggregate;
}

impl<A, B, C> Aggregation<(B, C)> for A
where
    A: Aggregation<B>,
    A::Output: Aggregation<C>,
{
    type Output = <A::Output as Aggregation<C>>::Output;
}

pub trait Expression {
    type SqlType;
    type Term;
    type BoolOperation;
    type Aggregation;
}

/// Only for cross join.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, Hash)]
pub struct BlankBoolExpression;

impl Expression for BlankBoolExpression {
    type SqlType = SqlTypeBool;
    type Term = Monomial;
    type BoolOperation = ();
    type Aggregation = NonAggregate;
}

impl BuildSql for BlankBoolExpression {
    fn build_sql(&self, _buf: &mut Vec<u8>, _params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum Order<E> {
    Asc(E),
    Desc(E),
}

impl<E> BuildSql for Order<E>
where
    E: BuildSql,
{
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        match self {
            Order::Asc(expr) => {
                expr.build_sql(buf, params)?;
                write!(buf, " ASC")?;
            }
            Order::Desc(expr) => {
                expr.build_sql(buf, params)?;
                write!(buf, " DESC")?;
            }
        }
        Ok(())
    }
}

pub trait Orders {}

impl<E> Orders for Order<E> where E: Expression {}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Limit {
    pub offset: Option<usize>,
    pub row_count: usize,
}

impl From<usize> for Limit {
    fn from(row_count: usize) -> Self {
        Limit {
            offset: None,
            row_count,
        }
    }
}

impl From<(usize, usize)> for Limit {
    fn from((offset, row_count): (usize, usize)) -> Self {
        Limit {
            offset: Some(offset),
            row_count,
        }
    }
}

impl BuildSql for Limit {
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        if let Some(offset) = self.offset {
            write!(buf, "?, ")?;
            params.push((offset as i64).into());
        }

        write!(buf, "?")?;
        params.push((self.row_count as i64).into());

        Ok(())
    }
}

pub trait Columns {
    type SqlType;
    type Aggregation;
}

impl<E: Expression> Columns for E {
    type SqlType = E::SqlType;
    type Aggregation = E::Aggregation;
}

#[derive(Debug, Clone)]
pub struct Distinct<T>(T);

impl<T> Distinct<T> {
    pub fn new(x: T) -> Distinct<T> {
        Distinct(x)
    }
}

impl<T> Columns for Distinct<T>
where
    T: Columns<Aggregation = NonAggregate>,
{
    type SqlType = T::SqlType;
    type Aggregation = T::Aggregation;
}

impl<T: BuildSql> BuildSql for Distinct<T> {
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        write!(buf, "DISTINCT ")?;
        self.0.build_sql(buf, params)
    }
}

/// Build SQL string as a part of SQL.
pub trait BuildSql {
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError>;
}

#[derive(Debug, thiserror::Error)]
pub enum BuildSqlError {
    #[error("Failed to build sql: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Failed to build sql: {0}")]
    AnyError(#[from] anyhow::Error),
}

impl<ST> BuildSql for Column<ST> {
    fn build_sql(&self, buf: &mut Vec<u8>, _params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        write!(buf, "{}.{}", self.table_name.to_string(), self.column_name)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Record<T> {
    columns: T,
}

impl<T> Record<T> {
    pub fn new(columns: T) -> Record<T> {
        Record { columns }
    }
}

impl<A> Columns for (A,)
where
    A: Expression,
{
    type SqlType = (A::SqlType,);
    type Aggregation = A::Aggregation;
}

impl<A: Expression> Expression for Record<(A,)> {
    type SqlType = A::SqlType;
    type Term = A::Term;
    type BoolOperation = A::BoolOperation;
    type Aggregation = A::Aggregation;
}

impl<A> Orders for (Order<A>,) where A: Expression {}

impl<A> BuildSql for (A,)
where
    A: BuildSql,
{
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        self.0.build_sql(buf, params)
    }
}

macro_rules! recursive_aggregation {
    ( $first_ty:ty, $( $ty:ty ),* $(,)* ) => {
        ($first_ty, recursive_aggregation!( $($ty,)* ))
    };

    () => {
        ()
    };
}

macro_rules! impl_traits_for_tuple {
    ($type_paramA:ident, $field0:tt $(,$type_param:ident, $field:tt)*) => {
        impl<$type_paramA $(, $type_param)*> Columns for ($type_paramA $(, $type_param)*)
        where
            $type_paramA: Columns,
            $($type_param: Columns,)*
            <$type_paramA>::Aggregation: Aggregation<recursive_aggregation!( $( <$type_param>::Aggregation, )* )>,
        {
            type SqlType = ( $type_paramA::SqlType, $( $type_param::SqlType, )* );
            type Aggregation = <<$type_paramA>::Aggregation as Aggregation<recursive_aggregation!( $( <$type_param>::Aggregation, )* )>>::Output;
        }

        impl<$type_paramA $(, $type_param)*> Expression for Record<($type_paramA $(, $type_param)*)>
        where
            $type_paramA: Expression,
            $($type_param: Expression,)*
            <$type_paramA>::Aggregation: Aggregation<recursive_aggregation!( $( <$type_param>::Aggregation, )* )>,
        {
            type SqlType = ( $type_paramA::SqlType, $( $type_param::SqlType, )* );
            type Term = Polynomial;
            type BoolOperation = BoolMono;
            type Aggregation = <<$type_paramA>::Aggregation as Aggregation<recursive_aggregation!( $( <$type_param>::Aggregation, )* )>>::Output;
        }

        impl<$type_paramA $(, $type_param)*> Orders for (Order<$type_paramA> $(, Order<$type_param>)*)
        where
            $type_paramA: Expression,
            $($type_param: Expression,)*
        {}

        impl<$type_paramA $(, $type_param)*> BuildSql for ($type_paramA $(, $type_param)*)
        where
            $type_paramA: BuildSql,
            $($type_param: BuildSql,)*
        {
            fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
                self.$field0.build_sql(buf, params)?;

                $(
                    write!(buf, ", ")?;
                    self.$field.build_sql(buf, params)?;
                )*

                Ok(())
            }
        }

        impl<$type_paramA $(, $type_param)*> BuildSql for Record<($type_paramA $(, $type_param)*)>
        where
            $type_paramA: BuildSql,
            $($type_param: BuildSql,)*
        {
            fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
                write!(buf, "(")?; // Rowって付けた方がいい？
                self.columns.$field0.build_sql(buf, params)?;
                $(
                    write!(buf, ", ")?;
                    self.columns.$field.build_sql(buf, params)?;
                )*
                write!(buf, ")")?;
                Ok(())
            }
        }
    }
}

// Increase if needed.
impl_traits_for_tuple!(A, 0, B, 1);
impl_traits_for_tuple!(A, 0, B, 1, C, 2);
impl_traits_for_tuple!(A, 0, B, 1, C, 2, D, 3);
impl_traits_for_tuple!(A, 0, B, 1, C, 2, D, 3, E, 4);
impl_traits_for_tuple!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5);
impl_traits_for_tuple!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6);
impl_traits_for_tuple!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7);
impl_traits_for_tuple!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8);
impl_traits_for_tuple!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9);
impl_traits_for_tuple!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10);
impl_traits_for_tuple!(A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42, AR, 43
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42, AR, 43, AS, 44
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42, AR, 43, AS, 44, AT, 45
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42, AR, 43, AS, 44, AT, 45, AU, 46
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42, AR, 43, AS, 44, AT, 45, AU, 46, AV, 47
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42, AR, 43, AS, 44, AT, 45, AU, 46, AV, 47, AW, 48
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42, AR, 43, AS, 44, AT, 45, AU, 46, AV, 47, AW, 48, AX, 49
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42, AR, 43, AS, 44, AT, 45, AU, 46, AV, 47, AW, 48, AX, 49, AY, 50
);
impl_traits_for_tuple!(
    A, 0, B, 1, C, 2, D, 3, E, 4, F, 5, G, 6, H, 7, I, 8, J, 9, K, 10, L, 11, M, 12, N, 13, O, 14,
    P, 15, Q, 16, R, 17, S, 18, T, 19, U, 20, V, 21, W, 22, X, 23, Y, 24, Z, 25, AA, 26, AB, 27,
    AC, 28, AD, 29, AE, 30, AF, 31, AG, 32, AH, 33, AI, 34, AJ, 35, AK, 36, AL, 37, AM, 38, AN, 39,
    AO, 40, AP, 41, AQ, 42, AR, 43, AS, 44, AT, 45, AU, 46, AV, 47, AW, 48, AX, 49, AY, 50, AZ, 51
);

#[derive(Debug, Clone)]
pub struct Row<T> {
    columns: T,
}

impl<T: Columns> Row<T> {
    pub fn new(columns: T) -> Row<T> {
        Row { columns }
    }
}

/// 事前にコードレベルでSQL上での型が確定できない場合に使用する。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Default)]
pub struct SqlTypeAny;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Default)]
pub struct SqlTypeString;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Default)]
pub struct SqlTypeInt;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Default)]
pub struct SqlTypeUint;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Default)]
pub struct SqlTypeBool;

pub trait Comparable<T> {}

impl Comparable<SqlTypeString> for SqlTypeAny {}

impl Comparable<SqlTypeInt> for SqlTypeAny {}

impl Comparable<SqlTypeUint> for SqlTypeAny {}

impl Comparable<SqlTypeBool> for SqlTypeAny {}

impl Comparable<SqlTypeAny> for SqlTypeString {}

impl Comparable<SqlTypeAny> for SqlTypeInt {}

impl Comparable<SqlTypeAny> for SqlTypeUint {}

impl Comparable<SqlTypeAny> for SqlTypeBool {}

impl Comparable<SqlTypeInt> for SqlTypeUint {}

impl Comparable<SqlTypeUint> for SqlTypeInt {}

impl<T> Comparable<T> for T {}

macro_rules! impl_bool_binary_operators {
    ( $( ( $ty:ident, $op:expr ) ),* $(,)* ) => {
        $(
            #[derive(Debug, Clone)]
            pub struct $ty<L, R>
            where
                L: Expression,
                R: Expression,
                L::SqlType: Comparable<R::SqlType>,
            {
                lhs: L,
                rhs: R,
            }

            impl<L, R> Expression for $ty<L, R>
            where
                L: Expression,
                R: Expression,
                L::SqlType: Comparable<R::SqlType>,
                L::Aggregation: Aggregation<R::Aggregation>,
            {
                type SqlType = SqlTypeBool;
                type Term = Polynomial;
                type BoolOperation = BoolMono;
                type Aggregation = <L::Aggregation as Aggregation<R::Aggregation>>::Output;
            }

            impl<L, R> AndOperatorMethod for $ty<L, R>
            where
                L: Expression,
                R: Expression,
                L::SqlType: Comparable<R::SqlType>,
                Self: Expression<SqlType = SqlTypeBool>,
            {}

            impl<L,R> OrOperatorMethod for $ty<L,R>
            where
                L: Expression,
                R: Expression,
                L::SqlType: Comparable<R::SqlType>,
                Self: Expression<SqlType = SqlTypeBool>,
            {}

            impl<L,R> NotOperatorMethod for $ty<L,R>
            where
                L: Expression,
                R: Expression,
                L::SqlType: Comparable<R::SqlType>,
                Self: Expression<SqlType = SqlTypeBool>,
            {}

            impl<L, R> BuildSql for $ty<L, R>
            where
                L: Expression + BuildSql,
                R: Expression + BuildSql,
                L::SqlType: Comparable<R::SqlType>,
            {
                fn build_sql(
                    &self,
                    buf: &mut Vec<u8>,
                    params: &mut Vec<Value>,
                ) -> Result<(), BuildSqlError> {
                    (|| -> Result<(), anyhow::Error> {
                        self.lhs.build_sql(buf, params)?;
                        write!(buf, $op)?;
                        self.rhs.build_sql(buf, params)?;
                        Ok(())
                    })()
                    .map_err(From::from)
                }
            }
        )*
    };
}

impl_bool_binary_operators!(
    (Eq, " = "),
    (NotEq, " != "),
    (Gt, " > "),
    (Ge, " >= "),
    (Lt, " < "),
    (Le, " <= "),
    (Like, " LIKE "),
    (NotLike, " NOT LIKE "),
);

macro_rules! impl_subquery_bool_binary_operators {
    ( $( ( $ty:ident, $op:expr ) ),* $(,)* ) => {
        $(
            #[derive(Debug, Clone)]
            pub struct $ty<Lhs, QS, W, C, G, H, O, L, LM>
            where
                C: Columns,
                Lhs: Expression,
                Lhs::SqlType: Comparable<C::SqlType>,
            {
                lhs: Lhs,
                rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
            }

            impl<Lhs, QS, W, C, G, H, O, L, LM> Expression for $ty<Lhs, QS, W, C, G, H, O, L, LM>
            where
                C: Columns,
                Lhs: Expression,
                Lhs::SqlType: Comparable<C::SqlType>,
            {
                type SqlType = SqlTypeBool;
                type Term = Polynomial;
                type BoolOperation = BoolMono;
                type Aggregation = NonAggregate;
            }

            impl<Lhs, QS, W, C, G, H, O, L, LM> AndOperatorMethod for $ty<Lhs, QS, W, C, G, H, O, L, LM>
            where
                C: Columns,
                Lhs: Expression,
                Lhs::SqlType: Comparable<C::SqlType>,
                Self: Expression<SqlType = SqlTypeBool>,
            {}

            impl<Lhs, QS, W, C, G, H, O, L, LM> OrOperatorMethod for $ty<Lhs, QS, W, C, G, H, O, L, LM>
            where
                C: Columns,
                Lhs: Expression,
                Lhs::SqlType: Comparable<C::SqlType>,
                Self: Expression<SqlType = SqlTypeBool>,
            {}

            impl<Lhs, QS, W, C, G, H, O, L, LM> NotOperatorMethod for $ty<Lhs, QS, W, C, G, H, O, L, LM>
            where
                C: Columns,
                Lhs: Expression,
                Lhs::SqlType: Comparable<C::SqlType>,
                Self: Expression<SqlType = SqlTypeBool>,
            {}

            impl<Lhs, QS, W, C, G, H, O, L, LM> BuildSql for $ty<Lhs, QS, W, C, G, H, O, L, LM>
            where
                QS: BuildSql,
                C: BuildSql + Columns,
                W: BuildSql,
                G: BuildSql,
                H: BuildSql,
                O: BuildSql,
                L: BuildSql,
                LM: BuildSql,
                Lhs: Expression + BuildSql,
                Lhs::SqlType: Comparable<C::SqlType>,
            {
                fn build_sql(
                    &self,
                    buf: &mut Vec<u8>,
                    params: &mut Vec<Value>,
                ) -> Result<(), BuildSqlError> {
                    (|| -> Result<(), anyhow::Error> {
                        self.lhs.build_sql(buf, params)?;
                        write!(buf, $op)?;
                        self.rhs.build_sql(buf, params)?;
                        Ok(())
                    })()
                    .map_err(From::from)
                }
            }
        )*
    };
}

impl_subquery_bool_binary_operators!(
    (EqAny, " = ANY "),
    (NotEqAny, " != ANY "),
    (GtAny, " > ANY "),
    (GeAny, " >= ANY "),
    (LtAny, " < ANY "),
    (LeAny, " <= ANY "),
    (EqAll, " = ALL "),
    (NotEqAll, " != ALL "),
    (GtAll, " > ALL "),
    (GeAll, " >= ALL "),
    (LtAll, " < ALL "),
    (LeAll, " <= ALL "),
);

#[derive(Debug, Clone)]
pub struct Between<T, L, U>
where
    T: Expression,
    L: Expression,
    U: Expression,
    L::SqlType: Comparable<T::SqlType>,
    U::SqlType: Comparable<T::SqlType>,
{
    target: T,
    lower_bound: L,
    upper_bound: U,
}

impl<T, L, U> Expression for Between<T, L, U>
where
    T: Expression,
    L: Expression,
    U: Expression,
    L::SqlType: Comparable<T::SqlType>,
    U::SqlType: Comparable<T::SqlType>,
{
    type SqlType = SqlTypeBool;
    type Term = Polynomial;
    type BoolOperation = BoolMono;
    type Aggregation = NonAggregate;
}

impl<T, L, U> AndOperatorMethod for Between<T, L, U>
where
    T: Expression,
    L: Expression,
    U: Expression,
    L::SqlType: Comparable<T::SqlType>,
    U::SqlType: Comparable<T::SqlType>,
    Self: Expression<SqlType = SqlTypeBool>,
{
}

impl<T, L, U> OrOperatorMethod for Between<T, L, U>
where
    T: Expression,
    L: Expression,
    U: Expression,
    L::SqlType: Comparable<T::SqlType>,
    U::SqlType: Comparable<T::SqlType>,
    Self: Expression<SqlType = SqlTypeBool>,
{
}

impl<T, L, U> NotOperatorMethod for Between<T, L, U>
where
    T: Expression,
    L: Expression,
    U: Expression,
    L::SqlType: Comparable<T::SqlType>,
    U::SqlType: Comparable<T::SqlType>,
    Self: Expression<SqlType = SqlTypeBool>,
{
}

impl<T, L, U> BuildSql for Between<T, L, U>
where
    T: Expression + BuildSql,
    L: Expression + BuildSql,
    U: Expression + BuildSql,
    L::SqlType: Comparable<T::SqlType>,
    U::SqlType: Comparable<T::SqlType>,
{
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        (|| -> Result<(), anyhow::Error> {
            self.target.build_sql(buf, params)?;
            write!(buf, " BETWEEN ")?;
            self.lower_bound.build_sql(buf, params)?;
            write!(buf, " AND ")?;
            self.upper_bound.build_sql(buf, params)?;
            Ok(())
        })()
        .map_err(From::from)
    }
}

macro_rules! impl_in_operators {
( $( ( $ty:ident, $op:expr ) ),* $(,)* ) => {
    $(
        #[derive(Debug, Clone)]
        pub struct $ty<L, R, ARR>
        where
            L: Expression,
            R: Expression,
            R::SqlType: Comparable<L::SqlType>,
            ARR: AsRef<[R]>,
        {
            lhs: L,
            rhs: ARR,
            rhs_value: PhantomData<R>,
        }

        impl<L, R, ARR> Expression for $ty<L, R, ARR>
        where
            L: Expression,
            R: Expression,
            R::SqlType: Comparable<L::SqlType>,
            ARR: AsRef<[R]>,
            R::Aggregation: Aggregation<L::Aggregation>,
        {
            type SqlType = SqlTypeBool;
            type Term = Polynomial;
            type BoolOperation = BoolMono;
            type Aggregation = <R::Aggregation as Aggregation<L::Aggregation>>::Output;
        }

        impl<L, R, ARR> AndOperatorMethod for $ty<L, R, ARR>
        where
            L: Expression,
            R: Expression,
            R::SqlType: Comparable<L::SqlType>,
            ARR: AsRef<[R]>,
            Self: Expression<SqlType = SqlTypeBool>,
        {}

        impl<L, R, ARR> OrOperatorMethod for $ty<L, R, ARR>
        where
            L: Expression,
            R: Expression,
            R::SqlType: Comparable<L::SqlType>,
            ARR: AsRef<[R]>,
            Self: Expression<SqlType = SqlTypeBool>,
        {}

        impl<L, R, ARR> NotOperatorMethod for $ty<L, R, ARR>
        where
            L: Expression,
            R: Expression,
            R::SqlType: Comparable<L::SqlType>,
            ARR: AsRef<[R]>,
            Self: Expression<SqlType = SqlTypeBool>,
        {}

        impl<L, R, ARR> BuildSql for $ty<L, R, ARR>
        where
            L: Expression + BuildSql,
            R: Expression + BuildSql + Clone + Into<Value>,
            R::SqlType: Comparable<L::SqlType>,
            ARR: AsRef<[R]>,
        {
            fn build_sql(
                &self,
                buf: &mut Vec<u8>,
                params: &mut Vec<Value>,
            ) -> Result<(), BuildSqlError> {
                (|| -> Result<(), anyhow::Error> {
                    self.lhs.build_sql(buf, params)?;
                    write!(buf, $op)?;
                    write!(buf, "(")?;
                    let mut is_first = true;
                    for v in self.rhs.as_ref() {
                        if is_first {
                            write!(buf, "?")?;
                            is_first = false;
                        } else {
                            write!(buf, ", ?")?;
                        }
                        params.push((*v).clone().into());
                    }
                    write!(buf, ")")?;
                    Ok(())
                })()
                .map_err(From::from)
            }
        }
    )*
};
}

impl_in_operators!((Any, " IN "), (NotAny, " NOT IN "));

macro_rules! impl_null_check_operators {
    ( $( ( $ty:ident, $op:expr ) ),* $(,)* ) => {
        $(
            #[derive(Debug, Clone)]
            pub struct $ty<T>
            where
                T: Expression,
            {
                target: T
            }

            impl<T> Expression for $ty<T>
            where
                T: Expression,
            {
                type SqlType = SqlTypeBool;
                type Term = Monomial;
                type BoolOperation = BoolMono;
                type Aggregation = T::Aggregation;
            }

            impl<T> AndOperatorMethod for $ty<T>
            where
                T: Expression,
                Self: Expression<SqlType = SqlTypeBool>,
            {}

            impl<T> OrOperatorMethod for $ty<T>
            where
                T: Expression,
                Self: Expression<SqlType = SqlTypeBool>,
            {}

            impl<T> NotOperatorMethod for $ty<T>
            where
                T: Expression,
                Self: Expression<SqlType = SqlTypeBool>,
            {}

            impl<T> BuildSql for $ty<T>
            where
                T: Expression + BuildSql,
            {
                fn build_sql(
                    &self,
                    buf: &mut Vec<u8>,
                    params: &mut Vec<Value>,
                ) -> Result<(), BuildSqlError> {
                    (|| -> Result<(), anyhow::Error> {
                        self.target.build_sql(buf, params)?;
                        write!(buf, $op)?;
                        Ok(())
                    })()
                    .map_err(From::from)
                }
            }
        )*
    };
}

impl_null_check_operators!((IsNull, " IS NULL"), (IsNotNull, " IS NOT NULL"));

pub trait CompareBinaryOperatorMethod<R>: Expression + Sized
where
    R: Expression,
    Self::SqlType: Comparable<R::SqlType>,
{
    /// SQL `=`.
    fn eq(self, rhs: R) -> Eq<Self, R> {
        Eq { lhs: self, rhs }
    }

    /// SQL `!=`.
    fn not_eq(self, rhs: R) -> NotEq<Self, R> {
        NotEq { lhs: self, rhs }
    }

    /// SQL `>`.
    fn gt(self, rhs: R) -> Gt<Self, R> {
        Gt { lhs: self, rhs }
    }

    /// SQL `>=`.
    fn ge(self, rhs: R) -> Ge<Self, R> {
        Ge { lhs: self, rhs }
    }

    /// SQL `<`.
    fn lt(self, rhs: R) -> Lt<Self, R> {
        Lt { lhs: self, rhs }
    }

    /// SQL `<=`.
    fn le(self, rhs: R) -> Le<Self, R> {
        Le { lhs: self, rhs }
    }

    /// SQL `LIKE`.
    fn like(self, rhs: R) -> Like<Self, R> {
        Like { lhs: self, rhs }
    }

    /// SQL `NOT LIKE`.
    fn not_like(self, rhs: R) -> NotLike<Self, R> {
        NotLike { lhs: self, rhs }
    }
}

// これをSelectBuilder毎に定義する必要がある
// DBによってサポートしてたりしてなかったりなので、それが正しいのかもしれない
pub trait SubQueryCompareBinaryOperatorMethod<QS, W, C, G, H, O, L, LM>:
    Expression + Sized
where
    C: Columns,
    Self::SqlType: Comparable<C::SqlType>,
{
    /// SQL `= ANY (...)`.
    fn eq_any(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> EqAny<Self, QS, W, C, G, H, O, L, LM> {
        EqAny { lhs: self, rhs }
    }

    /// SQL `!= ANY (...)`.
    fn not_eq_any(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> NotEqAny<Self, QS, W, C, G, H, O, L, LM> {
        NotEqAny { lhs: self, rhs }
    }

    /// SQL `> ANY (...)`.
    fn gt_any(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> GtAny<Self, QS, W, C, G, H, O, L, LM> {
        GtAny { lhs: self, rhs }
    }

    /// SQL `>= ANY (...)`.
    fn ge_any(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> GeAny<Self, QS, W, C, G, H, O, L, LM> {
        GeAny { lhs: self, rhs }
    }

    /// SQL `< ANY (...)`.
    fn lt_any(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> LtAny<Self, QS, W, C, G, H, O, L, LM> {
        LtAny { lhs: self, rhs }
    }

    /// SQL `<= ANY (...)`.
    fn le_any(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> LeAny<Self, QS, W, C, G, H, O, L, LM> {
        LeAny { lhs: self, rhs }
    }

    /// SQL `= ALL (...)`.
    fn eq_all(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> EqAll<Self, QS, W, C, G, H, O, L, LM> {
        EqAll { lhs: self, rhs }
    }

    /// SQL `!= ALL (...)`.
    fn not_eq_all(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> NotEqAll<Self, QS, W, C, G, H, O, L, LM> {
        NotEqAll { lhs: self, rhs }
    }

    /// SQL `> ALL (...)`.
    fn gt_all(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> GtAll<Self, QS, W, C, G, H, O, L, LM> {
        GtAll { lhs: self, rhs }
    }

    /// SQL `>= ALL (...)`.
    fn ge_all(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> GeAll<Self, QS, W, C, G, H, O, L, LM> {
        GeAll { lhs: self, rhs }
    }

    /// SQL `< ALL (...)`.
    fn lt_all(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> LtAll<Self, QS, W, C, G, H, O, L, LM> {
        LtAll { lhs: self, rhs }
    }

    /// SQL `<= ALL (...)`.
    fn le_all(
        self,
        rhs: SelectBuilder<QS, W, C, G, H, O, L, LM>,
    ) -> LeAll<Self, QS, W, C, G, H, O, L, LM> {
        LeAll { lhs: self, rhs }
    }
}

pub trait BetweenOperatorMethod<L, U>: Expression + Sized
where
    L: Expression,
    U: Expression,
    L::SqlType: Comparable<Self::SqlType>,
    U::SqlType: Comparable<Self::SqlType>,
{
    /// SQL `BETWEEN`.
    fn between(self, lower_bound: L, upper_bound: U) -> Between<Self, L, U> {
        Between {
            target: self,
            lower_bound,
            upper_bound,
        }
    }
}

pub trait InOperatorMethod<T>: Expression + Sized
where
    T: Expression,
    T::SqlType: Comparable<Self::SqlType>,
{
    /// SQL `IN`.
    fn any<V: AsRef<[T]>>(self, values: V) -> Any<Self, T, V> {
        Any {
            lhs: self,
            rhs: values,
            rhs_value: PhantomData,
        }
    }

    /// SQL `NOT IN`.
    fn not_any<V: AsRef<[T]>>(self, values: V) -> NotAny<Self, T, V> {
        NotAny {
            lhs: self,
            rhs: values,
            rhs_value: PhantomData,
        }
    }
}

pub trait NullCheckOperatorMethod: Expression + Sized {
    /// SQL `IS NULL`.
    #[allow(clippy::wrong_self_convention)]
    fn is_null(self) -> IsNull<Self> {
        IsNull { target: self }
    }

    /// SQL `IS NOT NULL`.
    #[allow(clippy::wrong_self_convention)]
    fn is_not_null(self) -> IsNotNull<Self> {
        IsNotNull { target: self }
    }
}

impl<L, R> CompareBinaryOperatorMethod<R> for L
where
    L: Expression,
    R: Expression,
    L::SqlType: Comparable<R::SqlType>,
{
}

impl<Lhs, QS, W, C, G, H, O, L, LM> SubQueryCompareBinaryOperatorMethod<QS, W, C, G, H, O, L, LM>
    for Lhs
where
    C: Columns,
    Lhs: Expression,
    Lhs::SqlType: Comparable<C::SqlType>,
{
}

impl<T, L, U> BetweenOperatorMethod<L, U> for T
where
    T: Expression,
    L: Expression,
    U: Expression,
    L::SqlType: Comparable<T::SqlType>,
    U::SqlType: Comparable<T::SqlType>,
{
}

impl<L, T> InOperatorMethod<T> for L
where
    L: Expression,
    T: Expression,
    T::SqlType: Comparable<L::SqlType>,
{
}

impl<T> NullCheckOperatorMethod for T where T: Expression {}

#[derive(Debug, Clone)]
pub struct And<L, LK, R, RK> {
    lhs: L,
    rhs: R,
    lhs_kind: PhantomData<LK>,
    rhs_kind: PhantomData<RK>,
}

impl<L, R> And<L, L::BoolOperation, R, R::BoolOperation>
where
    L: Expression,
    R: Expression,
{
    pub fn new(lhs: L, rhs: R) -> Self {
        And {
            lhs,
            rhs,
            lhs_kind: PhantomData,
            rhs_kind: PhantomData,
        }
    }
}

impl<L, R> Expression for And<L, L::BoolOperation, R, R::BoolOperation>
where
    L: Expression,
    R: Expression,
    L::SqlType: Comparable<SqlTypeBool>,
    R::SqlType: Comparable<L::SqlType>,
    L::Aggregation: Aggregation<R::Aggregation>,
{
    type SqlType = SqlTypeBool;
    type Term = Polynomial;
    type BoolOperation = BoolAnd;
    type Aggregation = <L::Aggregation as Aggregation<R::Aggregation>>::Output;
}

impl<L, R> AndOperatorMethod for And<L, L::BoolOperation, R, R::BoolOperation>
where
    L: Expression,
    R: Expression,
    L::SqlType: Comparable<SqlTypeBool>,
    R::SqlType: Comparable<L::SqlType>,
    Self: Expression<SqlType = SqlTypeBool>,
{
}

impl<L, R> NotOperatorMethod for And<L, L::BoolOperation, R, R::BoolOperation>
where
    L: Expression,
    R: Expression,
    L::SqlType: Comparable<SqlTypeBool>,
    R::SqlType: Comparable<L::SqlType>,
    Self: Expression<SqlType = SqlTypeBool>,
{
}

#[derive(Debug, Clone)]
pub struct Or<L, LK, R, RK> {
    lhs: L,
    rhs: R,
    lhs_kind: PhantomData<LK>,
    rhs_kind: PhantomData<RK>,
}

impl<L, R> Or<L, L::BoolOperation, R, R::BoolOperation>
where
    L: Expression,
    R: Expression,
{
    pub fn new(lhs: L, rhs: R) -> Self {
        Or {
            lhs,
            rhs,
            lhs_kind: PhantomData,
            rhs_kind: PhantomData,
        }
    }
}

impl<L, R> Expression for Or<L, L::BoolOperation, R, R::BoolOperation>
where
    L: Expression,
    R: Expression,
    L::SqlType: Comparable<SqlTypeBool>,
    R::SqlType: Comparable<L::SqlType>,
    L::Aggregation: Aggregation<R::Aggregation>,
{
    type SqlType = SqlTypeBool;
    type Term = Polynomial;
    type BoolOperation = BoolOr;
    type Aggregation = <L::Aggregation as Aggregation<R::Aggregation>>::Output;
}

impl<L, R> OrOperatorMethod for Or<L, L::BoolOperation, R, R::BoolOperation>
where
    L: Expression,
    R: Expression,
    L::SqlType: Comparable<SqlTypeBool>,
    R::SqlType: Comparable<L::SqlType>,
    Self: Expression<SqlType = SqlTypeBool>,
{
}

impl<L, R> NotOperatorMethod for Or<L, L::BoolOperation, R, R::BoolOperation>
where
    L: Expression,
    R: Expression,
    L::SqlType: Comparable<SqlTypeBool>,
    R::SqlType: Comparable<L::SqlType>,
    Self: Expression<SqlType = SqlTypeBool>,
{
}

macro_rules! impl_build_sql_and_or {
    ( $ty:ident, $op:expr, both_no_parentheses, $l_kind:ty, $r_kind:ty ) => {
        impl<L, R> BuildSql for $ty<L, $l_kind, R, $r_kind>
        where
            L: Expression<BoolOperation = $l_kind> + BuildSql,
            R: Expression<BoolOperation = $r_kind> + BuildSql,
            L::SqlType: Comparable<SqlTypeBool>,
            R::SqlType: Comparable<SqlTypeBool>,
        {
            fn build_sql(
                &self,
                buf: &mut Vec<u8>,
                params: &mut Vec<Value>,
            ) -> Result<(), BuildSqlError> {
                (|| -> Result<(), anyhow::Error> {
                    self.lhs.build_sql(buf, params)?;
                    write!(buf, $op)?;
                    self.rhs.build_sql(buf, params)?;
                    Ok(())
                })()
                .map_err(From::from)
            }
        }
    };
    ( $ty:ident, $op:expr, lhs_parentheses, $l_kind:ty, $r_kind:ty ) => {
        impl<L, R> BuildSql for $ty<L, $l_kind, R, $r_kind>
        where
            L: Expression<BoolOperation = $l_kind> + BuildSql,
            R: Expression<BoolOperation = $r_kind> + BuildSql,
            L::SqlType: Comparable<SqlTypeBool>,
            R::SqlType: Comparable<SqlTypeBool>,
        {
            fn build_sql(
                &self,
                buf: &mut Vec<u8>,
                params: &mut Vec<Value>,
            ) -> Result<(), BuildSqlError> {
                (|| -> Result<(), anyhow::Error> {
                    write!(buf, "(")?;
                    self.lhs.build_sql(buf, params)?;
                    write!(buf, ")")?;
                    write!(buf, $op)?;
                    self.rhs.build_sql(buf, params)?;
                    Ok(())
                })()
                .map_err(From::from)
            }
        }
    };
    ( $ty:ident, $op:expr, rhs_parentheses, $l_kind:ty, $r_kind:ty ) => {
        impl<L, R> BuildSql for $ty<L, $l_kind, R, $r_kind>
        where
            L: Expression<BoolOperation = $l_kind> + BuildSql,
            R: Expression<BoolOperation = $r_kind> + BuildSql,
            L::SqlType: Comparable<SqlTypeBool>,
            R::SqlType: Comparable<SqlTypeBool>,
        {
            fn build_sql(
                &self,
                buf: &mut Vec<u8>,
                params: &mut Vec<Value>,
            ) -> Result<(), BuildSqlError> {
                (|| -> Result<(), anyhow::Error> {
                    self.lhs.build_sql(buf, params)?;
                    write!(buf, $op)?;
                    write!(buf, "(")?;
                    self.rhs.build_sql(buf, params)?;
                    write!(buf, ")")?;
                    Ok(())
                })()
                .map_err(From::from)
            }
        }
    };

    ( $ty:ident, $op:expr, both_parentheses, $l_kind:ty, $r_kind:ty ) => {
        impl<L, R> BuildSql for $ty<L, $l_kind, R, $r_kind>
        where
            L: Expression<BoolOperation = $l_kind> + BuildSql,
            R: Expression<BoolOperation = $r_kind> + BuildSql,
            L::SqlType: Comparable<SqlTypeBool>,
            R::SqlType: Comparable<SqlTypeBool>,
        {
            fn build_sql(
                &self,
                buf: &mut Vec<u8>,
                params: &mut Vec<Value>,
            ) -> Result<(), BuildSqlError> {
                (|| -> Result<(), anyhow::Error> {
                    write!(buf, "(")?;
                    self.lhs.build_sql(buf, params)?;
                    write!(buf, ")")?;
                    write!(buf, $op)?;
                    write!(buf, "(")?;
                    self.rhs.build_sql(buf, params)?;
                    write!(buf, ")")?;
                    Ok(())
                })()
                .map_err(From::from)
            }
        }
    };
}

impl_build_sql_and_or!(And, " AND ", both_no_parentheses, BoolMono, BoolMono);
impl_build_sql_and_or!(And, " AND ", both_no_parentheses, BoolMono, BoolAnd);
impl_build_sql_and_or!(And, " AND ", rhs_parentheses, BoolMono, BoolOr);
impl_build_sql_and_or!(And, " AND ", both_no_parentheses, BoolAnd, BoolMono);
impl_build_sql_and_or!(And, " AND ", both_no_parentheses, BoolAnd, BoolAnd);
impl_build_sql_and_or!(And, " AND ", rhs_parentheses, BoolAnd, BoolOr);
impl_build_sql_and_or!(And, " AND ", lhs_parentheses, BoolOr, BoolMono);
impl_build_sql_and_or!(And, " AND ", lhs_parentheses, BoolOr, BoolAnd);
impl_build_sql_and_or!(And, " AND ", both_parentheses, BoolOr, BoolOr);

impl_build_sql_and_or!(Or, " OR ", both_no_parentheses, BoolMono, BoolMono);
impl_build_sql_and_or!(Or, " OR ", rhs_parentheses, BoolMono, BoolAnd);
impl_build_sql_and_or!(Or, " OR ", both_no_parentheses, BoolMono, BoolOr);
impl_build_sql_and_or!(Or, " OR ", lhs_parentheses, BoolAnd, BoolMono);
impl_build_sql_and_or!(Or, " OR ", both_parentheses, BoolAnd, BoolAnd);
impl_build_sql_and_or!(Or, " OR ", lhs_parentheses, BoolAnd, BoolOr);
impl_build_sql_and_or!(Or, " OR ", both_no_parentheses, BoolOr, BoolMono);
impl_build_sql_and_or!(Or, " OR ", rhs_parentheses, BoolOr, BoolAnd);
impl_build_sql_and_or!(Or, " OR ", both_no_parentheses, BoolOr, BoolOr);

#[derive(Debug, Clone)]
pub struct Not<T, S> {
    expr: T,
    expr_term: PhantomData<S>,
}

impl<T> Not<T, T::Term>
where
    T: Expression,
    T::SqlType: Comparable<SqlTypeBool>,
{
    pub fn new(expr: T) -> Not<T, T::Term> {
        Not {
            expr,
            expr_term: PhantomData,
        }
    }
}

impl<T> Expression for Not<T, T::Term>
where
    T: Expression,
    T::SqlType: Comparable<SqlTypeBool>,
{
    type SqlType = SqlTypeBool;
    type Term = Monomial;
    type BoolOperation = BoolMono;
    type Aggregation = T::Aggregation;
}

impl<T> BuildSql for Not<T, Monomial>
where
    T: Expression<Term = Monomial> + BuildSql,
    T::SqlType: Comparable<SqlTypeBool>,
{
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        (|| -> Result<(), anyhow::Error> {
            write!(buf, "NOT ")?;
            self.expr.build_sql(buf, params)?;
            Ok(())
        })()
        .map_err(From::from)
    }
}

impl<T> BuildSql for Not<T, Polynomial>
where
    T: Expression<Term = Polynomial> + BuildSql,
    T::SqlType: Comparable<SqlTypeBool>,
{
    fn build_sql(&self, buf: &mut Vec<u8>, params: &mut Vec<Value>) -> Result<(), BuildSqlError> {
        (|| -> Result<(), anyhow::Error> {
            write!(buf, "NOT (")?;
            self.expr.build_sql(buf, params)?;
            write!(buf, ")")?;
            Ok(())
        })()
        .map_err(From::from)
    }
}

pub trait AndOperatorMethod: Expression + Sized
where
    Self::SqlType: Comparable<SqlTypeBool>,
{
    fn and<R>(self, rhs: R) -> And<Self, Self::BoolOperation, R, R::BoolOperation>
    where
        R: Expression,
        R::SqlType: Comparable<Self::SqlType>,
    {
        And::new(self, rhs)
    }
}

pub trait OrOperatorMethod: Expression + Sized
where
    Self::SqlType: Comparable<SqlTypeBool>,
{
    fn or<R>(self, rhs: R) -> Or<Self, Self::BoolOperation, R, R::BoolOperation>
    where
        R: Expression,
        R::SqlType: Comparable<Self::SqlType>,
    {
        Or::new(self, rhs)
    }
}

pub trait NotOperatorMethod: Expression + Sized
where
    Self::SqlType: Comparable<SqlTypeBool>,
{
    fn not(self) -> Not<Self, Self::Term> {
        Not::new(self)
    }
}

pub fn not<T: NotOperatorMethod + Expression>(expr: T) -> Not<T, T::Term>
where
    T: NotOperatorMethod + Expression,
    T::SqlType: Comparable<SqlTypeBool>,
{
    expr.not()
}

macro_rules! define_sql_function {
    ( $func_type:ident, $func_name:ident ( $( $arg_name:ident : $arg_type:ty ),* ) -> $ret_type:ty, $aggregation:ty ) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone)]
        pub struct $func_type< $( $arg_name, )* > {
            $( $arg_name: $arg_name, )*
        }

        #[allow(non_camel_case_types)]
        impl<$( $arg_name, )*> $func_type<$( $arg_name, )*> {
            pub fn new( $( $arg_name: $arg_name, )* ) -> $func_type<$( $arg_name, )*> {
                $func_type { $( $arg_name, )* }
            }
        }

        #[allow(non_camel_case_types)]
        pub fn $func_name<$( $arg_name, )*>( $( $arg_name: $arg_name, )* ) -> $func_type<$( $arg_name, )*>
        where
            $(
                $arg_name: Expression,
                <$arg_name>::SqlType: Comparable<$arg_type>,
            )*
        {
            $func_type::new($( $arg_name, )*)
        }

        #[allow(non_camel_case_types)]
        impl<$( $arg_name, )*> Expression for $func_type<$( $arg_name, )*>
        where
            $(
                $arg_name: Expression,
                <$arg_name>::SqlType: Comparable<$arg_type>,
            )*
        {
            type SqlType = $ret_type;
            type Term = Monomial;
            type BoolOperation = NonBool;
            type Aggregation = $aggregation;
        }

        #[allow(non_camel_case_types)]
        impl<$( $arg_name, )*> BuildSql for $func_type<$( $arg_name, )*>
        where
            $( $arg_name: BuildSql, )*
        {
            fn build_sql(
                &self,
                buf: &mut Vec<u8>,
                params: &mut Vec<Value>,
            ) -> Result<(), BuildSqlError> {
                write!(buf, concat!(stringify!($func_name), "("))?;
                build_sql_comma_separated_values!(buf, params, self, $( $arg_name, )*);
                write!(buf, ")")?;
                Ok(())
            }
        }
    };
}

macro_rules! build_sql_comma_separated_values {
    ( $buf:ident, $params:ident, $x:ident, $first_field:ident $(, $field:ident )* $(,)* ) => {
        $x.$first_field.build_sql($buf, $params)?;
        $(
            write!($buf, ", ")?;
            $x.$field.build_sql($buf, $params)?;
        )*
    };
    ( $x:ident, ) => {};
}

// SQLは動的型付けなので関数も動的な型に対応できる必要がある。
// 例えばsumは整数型にも実数型にも使えるので、複数の型を取り得る。
// なので関数はtraitとして実装した方が良いのではないか？
define_sql_function!(Sum, sum(t: SqlTypeInt) -> SqlTypeInt, Aggregate);
define_sql_function!(Count, count(t: SqlTypeAny) -> SqlTypeInt, Aggregate);

define_sql_function!(Date, date(t: SqlTypeString) -> SqlTypeString, NonAggregate);
define_sql_function!(Left, left(t: SqlTypeString, n: SqlTypeInt) -> SqlTypeString, NonAggregate);
