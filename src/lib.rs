//! AQUERYON - A SQL Builder for Rust
//!

#![deny(warnings, clippy::all)]
//#![deny(missing_docs)]

pub mod query_builder;
pub mod param {
    // query_builderのVec<Value>を各クライアントライブラリのparam用の型に変換するためのtraitとその実装

    // pub trait ToMysqlParam {
    //     fn to_mysql_param(&self);
    // }
    //
    // pub trait ToPostgresParam {
    //     fn to_postgres_param(&self);
    // }
    //
    // pub trait ToSQLiteParam {{
    //     fn to_sqlite_param(&self);
    // }
}

#[cfg(test)]
mod tests;
