mod apply;
mod batched_csv;
mod conversion;
mod dataframe;
mod error;
mod file;
mod lazy;
mod list_construction;
mod series;
mod set;
mod utils;

use batched_csv::RbBatchedCsv;
use conversion::*;
use dataframe::RbDataFrame;
use error::{RbPolarsErr, RbValueError};
use file::get_file_like;
use lazy::dataframe::{RbLazyFrame, RbLazyGroupBy};
use lazy::dsl::{RbExpr, RbWhen, RbWhenThen};
use lazy::utils::rb_exprs_to_exprs;
use magnus::{
    define_module, function, memoize, method, prelude::*, Error, RArray, RClass, RHash, RModule,
    Value,
};
use polars::datatypes::{DataType, TimeUnit};
use polars::error::PolarsResult;
use polars::frame::DataFrame;
use polars::functions::{diag_concat_df, hor_concat_df};
use polars::prelude::{ClosedWindow, Duration, DurationArgs, IntoSeries, TimeZone};
use series::RbSeries;

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[cfg(not(target_os = "linux"))]
use mimalloc::MiMalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static GLOBAL: Jemalloc = Jemalloc;

#[global_allocator]
#[cfg(not(target_os = "linux"))]
static GLOBAL: MiMalloc = MiMalloc;

type RbResult<T> = Result<T, Error>;

fn module() -> RModule {
    *memoize!(RModule: define_module("Polars").unwrap())
}

fn series() -> RClass {
    *memoize!(RClass: module().define_class("Series", Default::default()).unwrap())
}

#[magnus::init]
fn init() -> RbResult<()> {
    let module = module();
    module.define_singleton_method("_dtype_cols", function!(dtype_cols, 1))?;
    module.define_singleton_method("_rb_duration", function!(rb_duration, 8))?;
    module.define_singleton_method("_concat_df", function!(concat_df, 1))?;
    module.define_singleton_method("_concat_lf", function!(concat_lf, 3))?;
    module.define_singleton_method("_diag_concat_df", function!(rb_diag_concat_df, 1))?;
    module.define_singleton_method("_hor_concat_df", function!(rb_hor_concat_df, 1))?;
    module.define_singleton_method("_concat_series", function!(concat_series, 1))?;
    module.define_singleton_method("_ipc_schema", function!(ipc_schema, 1))?;
    module.define_singleton_method("_parquet_schema", function!(parquet_schema, 1))?;
    module.define_singleton_method("_collect_all", function!(collect_all, 1))?;
    module.define_singleton_method("_rb_date_range", function!(rb_date_range, 7))?;
    module.define_singleton_method("_coalesce_exprs", function!(coalesce_exprs, 1))?;
    module.define_singleton_method("_sum_exprs", function!(sum_exprs, 1))?;
    module.define_singleton_method("_as_struct", function!(as_struct, 1))?;
    module.define_singleton_method("_arg_where", function!(arg_where, 1))?;

    let class = module.define_class("RbBatchedCsv", Default::default())?;
    class.define_singleton_method("new", function!(RbBatchedCsv::new, -1))?;
    class.define_method("next_batches", method!(RbBatchedCsv::next_batches, 1))?;

    let class = module.define_class("RbDataFrame", Default::default())?;
    class.define_singleton_method("new", function!(RbDataFrame::init, 1))?;
    class.define_singleton_method("read_csv", function!(RbDataFrame::read_csv, -1))?;
    class.define_singleton_method("read_parquet", function!(RbDataFrame::read_parquet, 7))?;
    class.define_singleton_method("read_ipc", function!(RbDataFrame::read_ipc, 6))?;
    class.define_singleton_method("read_avro", function!(RbDataFrame::read_avro, 4))?;
    class.define_singleton_method("read_hashes", function!(RbDataFrame::read_hashes, 3))?;
    class.define_singleton_method("read_hash", function!(RbDataFrame::read_hash, 1))?;
    class.define_singleton_method("read_json", function!(RbDataFrame::read_json, 1))?;
    class.define_singleton_method("read_ndjson", function!(RbDataFrame::read_ndjson, 1))?;
    class.define_method("estimated_size", method!(RbDataFrame::estimated_size, 0))?;
    class.define_method("write_avro", method!(RbDataFrame::write_avro, 2))?;
    class.define_method("write_json", method!(RbDataFrame::write_json, 3))?;
    class.define_method("write_ndjson", method!(RbDataFrame::write_ndjson, 1))?;
    class.define_method("write_csv", method!(RbDataFrame::write_csv, 10))?;
    class.define_method("write_ipc", method!(RbDataFrame::write_ipc, 2))?;
    class.define_method("row_tuple", method!(RbDataFrame::row_tuple, 1))?;
    class.define_method("row_tuples", method!(RbDataFrame::row_tuples, 0))?;
    class.define_method("write_parquet", method!(RbDataFrame::write_parquet, 5))?;
    class.define_method("add", method!(RbDataFrame::add, 1))?;
    class.define_method("sub", method!(RbDataFrame::sub, 1))?;
    class.define_method("div", method!(RbDataFrame::div, 1))?;
    class.define_method("mul", method!(RbDataFrame::mul, 1))?;
    class.define_method("rem", method!(RbDataFrame::rem, 1))?;
    class.define_method("add_df", method!(RbDataFrame::add_df, 1))?;
    class.define_method("sub_df", method!(RbDataFrame::sub_df, 1))?;
    class.define_method("div_df", method!(RbDataFrame::div_df, 1))?;
    class.define_method("mul_df", method!(RbDataFrame::mul_df, 1))?;
    class.define_method("rem_df", method!(RbDataFrame::rem_df, 1))?;
    class.define_method("sample_n", method!(RbDataFrame::sample_n, 4))?;
    class.define_method("sample_frac", method!(RbDataFrame::sample_frac, 4))?;
    class.define_method("rechunk", method!(RbDataFrame::rechunk, 0))?;
    class.define_method("to_s", method!(RbDataFrame::to_s, 0))?;
    class.define_method("get_columns", method!(RbDataFrame::get_columns, 0))?;
    class.define_method("columns", method!(RbDataFrame::columns, 0))?;
    class.define_method(
        "set_column_names",
        method!(RbDataFrame::set_column_names, 1),
    )?;
    class.define_method("dtypes", method!(RbDataFrame::dtypes, 0))?;
    class.define_method("n_chunks", method!(RbDataFrame::n_chunks, 0))?;
    class.define_method("shape", method!(RbDataFrame::shape, 0))?;
    class.define_method("height", method!(RbDataFrame::height, 0))?;
    class.define_method("width", method!(RbDataFrame::width, 0))?;
    class.define_method("hstack_mut", method!(RbDataFrame::hstack_mut, 1))?;
    class.define_method("hstack", method!(RbDataFrame::hstack, 1))?;
    class.define_method("extend", method!(RbDataFrame::extend, 1))?;
    class.define_method("vstack_mut", method!(RbDataFrame::vstack_mut, 1))?;
    class.define_method("vstack", method!(RbDataFrame::vstack, 1))?;
    class.define_method("drop_in_place", method!(RbDataFrame::drop_in_place, 1))?;
    class.define_method("drop_nulls", method!(RbDataFrame::drop_nulls, 1))?;
    class.define_method("drop", method!(RbDataFrame::drop, 1))?;
    class.define_method("select_at_idx", method!(RbDataFrame::select_at_idx, 1))?;
    class.define_method(
        "find_idx_by_name",
        method!(RbDataFrame::find_idx_by_name, 1),
    )?;
    class.define_method("column", method!(RbDataFrame::column, 1))?;
    class.define_method("select", method!(RbDataFrame::select, 1))?;
    class.define_method("take", method!(RbDataFrame::take, 1))?;
    class.define_method(
        "take_with_series",
        method!(RbDataFrame::take_with_series, 1),
    )?;
    class.define_method("sort", method!(RbDataFrame::sort, 3))?;
    class.define_method("replace", method!(RbDataFrame::replace, 2))?;
    class.define_method("replace_at_idx", method!(RbDataFrame::replace_at_idx, 2))?;
    class.define_method("insert_at_idx", method!(RbDataFrame::insert_at_idx, 2))?;
    class.define_method("slice", method!(RbDataFrame::slice, 2))?;
    class.define_method("head", method!(RbDataFrame::head, 1))?;
    class.define_method("tail", method!(RbDataFrame::tail, 1))?;
    class.define_method("is_unique", method!(RbDataFrame::is_unique, 0))?;
    class.define_method("is_duplicated", method!(RbDataFrame::is_duplicated, 0))?;
    class.define_method("frame_equal", method!(RbDataFrame::frame_equal, 2))?;
    class.define_method("with_row_count", method!(RbDataFrame::with_row_count, 2))?;
    class.define_method("_clone", method!(RbDataFrame::clone, 0))?;
    class.define_method("melt", method!(RbDataFrame::melt, 4))?;
    class.define_method("pivot_expr", method!(RbDataFrame::pivot_expr, 6))?;
    class.define_method("partition_by", method!(RbDataFrame::partition_by, 2))?;
    class.define_method("shift", method!(RbDataFrame::shift, 1))?;
    class.define_method("unique", method!(RbDataFrame::unique, 3))?;
    class.define_method("lazy", method!(RbDataFrame::lazy, 0))?;
    class.define_method("max", method!(RbDataFrame::max, 0))?;
    class.define_method("min", method!(RbDataFrame::min, 0))?;
    class.define_method("sum", method!(RbDataFrame::sum, 0))?;
    class.define_method("mean", method!(RbDataFrame::mean, 0))?;
    class.define_method("std", method!(RbDataFrame::std, 1))?;
    class.define_method("var", method!(RbDataFrame::var, 1))?;
    class.define_method("median", method!(RbDataFrame::median, 0))?;
    class.define_method("hmean", method!(RbDataFrame::hmean, 1))?;
    class.define_method("hmax", method!(RbDataFrame::hmax, 0))?;
    class.define_method("hmin", method!(RbDataFrame::hmin, 0))?;
    class.define_method("hsum", method!(RbDataFrame::hsum, 1))?;
    class.define_method("quantile", method!(RbDataFrame::quantile, 2))?;
    class.define_method("to_dummies", method!(RbDataFrame::to_dummies, 1))?;
    class.define_method("null_count", method!(RbDataFrame::null_count, 0))?;
    class.define_method("apply", method!(RbDataFrame::apply, 3))?;
    class.define_method("shrink_to_fit", method!(RbDataFrame::shrink_to_fit, 0))?;
    class.define_method("hash_rows", method!(RbDataFrame::hash_rows, 4))?;
    class.define_method("transpose", method!(RbDataFrame::transpose, 2))?;
    class.define_method("upsample", method!(RbDataFrame::upsample, 5))?;
    class.define_method("to_struct", method!(RbDataFrame::to_struct, 1))?;
    class.define_method("unnest", method!(RbDataFrame::unnest, 1))?;

    let class = module.define_class("RbExpr", Default::default())?;
    class.define_method("+", method!(RbExpr::add, 1))?;
    class.define_method("-", method!(RbExpr::sub, 1))?;
    class.define_method("*", method!(RbExpr::mul, 1))?;
    class.define_method("/", method!(RbExpr::truediv, 1))?;
    class.define_method("%", method!(RbExpr::_mod, 1))?;
    class.define_method("floordiv", method!(RbExpr::floordiv, 1))?;
    class.define_method("to_str", method!(RbExpr::to_str, 0))?;
    class.define_method("eq", method!(RbExpr::eq, 1))?;
    class.define_method("neq", method!(RbExpr::neq, 1))?;
    class.define_method("gt", method!(RbExpr::gt, 1))?;
    class.define_method("gt_eq", method!(RbExpr::gt_eq, 1))?;
    class.define_method("lt_eq", method!(RbExpr::lt_eq, 1))?;
    class.define_method("lt", method!(RbExpr::lt, 1))?;
    class.define_method("_alias", method!(RbExpr::alias, 1))?;
    class.define_method("is_not", method!(RbExpr::is_not, 0))?;
    class.define_method("is_null", method!(RbExpr::is_null, 0))?;
    class.define_method("is_not_null", method!(RbExpr::is_not_null, 0))?;
    class.define_method("is_infinite", method!(RbExpr::is_infinite, 0))?;
    class.define_method("is_finite", method!(RbExpr::is_finite, 0))?;
    class.define_method("is_nan", method!(RbExpr::is_nan, 0))?;
    class.define_method("is_not_nan", method!(RbExpr::is_not_nan, 0))?;
    class.define_method("min", method!(RbExpr::min, 0))?;
    class.define_method("max", method!(RbExpr::max, 0))?;
    class.define_method("nan_max", method!(RbExpr::nan_max, 0))?;
    class.define_method("nan_min", method!(RbExpr::nan_min, 0))?;
    class.define_method("mean", method!(RbExpr::mean, 0))?;
    class.define_method("median", method!(RbExpr::median, 0))?;
    class.define_method("sum", method!(RbExpr::sum, 0))?;
    class.define_method("n_unique", method!(RbExpr::n_unique, 0))?;
    class.define_method("arg_unique", method!(RbExpr::arg_unique, 0))?;
    class.define_method("unique", method!(RbExpr::unique, 0))?;
    class.define_method("unique_stable", method!(RbExpr::unique_stable, 0))?;
    class.define_method("first", method!(RbExpr::first, 0))?;
    class.define_method("last", method!(RbExpr::last, 0))?;
    class.define_method("list", method!(RbExpr::list, 0))?;
    class.define_method("quantile", method!(RbExpr::quantile, 2))?;
    class.define_method("agg_groups", method!(RbExpr::agg_groups, 0))?;
    class.define_method("count", method!(RbExpr::count, 0))?;
    class.define_method("value_counts", method!(RbExpr::value_counts, 2))?;
    class.define_method("unique_counts", method!(RbExpr::unique_counts, 0))?;
    class.define_method("null_count", method!(RbExpr::null_count, 0))?;
    class.define_method("cast", method!(RbExpr::cast, 2))?;
    class.define_method("sort_with", method!(RbExpr::sort_with, 2))?;
    class.define_method("arg_sort", method!(RbExpr::arg_sort, 2))?;
    class.define_method("top_k", method!(RbExpr::top_k, 2))?;
    class.define_method("arg_max", method!(RbExpr::arg_max, 0))?;
    class.define_method("arg_min", method!(RbExpr::arg_min, 0))?;
    class.define_method("search_sorted", method!(RbExpr::search_sorted, 1))?;
    class.define_method("take", method!(RbExpr::take, 1))?;
    class.define_method("sort_by", method!(RbExpr::sort_by, 2))?;
    class.define_method("backward_fill", method!(RbExpr::backward_fill, 1))?;
    class.define_method("forward_fill", method!(RbExpr::forward_fill, 1))?;
    class.define_method("shift", method!(RbExpr::shift, 1))?;
    class.define_method("shift_and_fill", method!(RbExpr::shift_and_fill, 2))?;
    class.define_method("fill_null", method!(RbExpr::fill_null, 1))?;
    class.define_method(
        "fill_null_with_strategy",
        method!(RbExpr::fill_null_with_strategy, 2),
    )?;
    class.define_method("fill_nan", method!(RbExpr::fill_nan, 1))?;
    class.define_method("drop_nulls", method!(RbExpr::drop_nulls, 0))?;
    class.define_method("drop_nans", method!(RbExpr::drop_nans, 0))?;
    class.define_method("filter", method!(RbExpr::filter, 1))?;
    class.define_method("reverse", method!(RbExpr::reverse, 0))?;
    class.define_method("std", method!(RbExpr::std, 1))?;
    class.define_method("var", method!(RbExpr::var, 1))?;
    class.define_method("is_unique", method!(RbExpr::is_unique, 0))?;
    class.define_method("is_first", method!(RbExpr::is_first, 0))?;
    class.define_method("explode", method!(RbExpr::explode, 0))?;
    class.define_method("take_every", method!(RbExpr::take_every, 1))?;
    class.define_method("tail", method!(RbExpr::tail, 1))?;
    class.define_method("head", method!(RbExpr::head, 1))?;
    class.define_method("slice", method!(RbExpr::slice, 2))?;
    class.define_method("append", method!(RbExpr::append, 2))?;
    class.define_method("rechunk", method!(RbExpr::rechunk, 0))?;
    class.define_method("round", method!(RbExpr::round, 1))?;
    class.define_method("floor", method!(RbExpr::floor, 0))?;
    class.define_method("ceil", method!(RbExpr::ceil, 0))?;
    class.define_method("clip", method!(RbExpr::clip, 2))?;
    class.define_method("clip_min", method!(RbExpr::clip_min, 1))?;
    class.define_method("clip_max", method!(RbExpr::clip_max, 1))?;
    class.define_method("abs", method!(RbExpr::abs, 0))?;
    class.define_method("sin", method!(RbExpr::sin, 0))?;
    class.define_method("cos", method!(RbExpr::cos, 0))?;
    class.define_method("tan", method!(RbExpr::tan, 0))?;
    class.define_method("arcsin", method!(RbExpr::arcsin, 0))?;
    class.define_method("arccos", method!(RbExpr::arccos, 0))?;
    class.define_method("arctan", method!(RbExpr::arctan, 0))?;
    class.define_method("sinh", method!(RbExpr::sinh, 0))?;
    class.define_method("cosh", method!(RbExpr::cosh, 0))?;
    class.define_method("tanh", method!(RbExpr::tanh, 0))?;
    class.define_method("arcsinh", method!(RbExpr::arcsinh, 0))?;
    class.define_method("arccosh", method!(RbExpr::arccosh, 0))?;
    class.define_method("arctanh", method!(RbExpr::arctanh, 0))?;
    class.define_method("sign", method!(RbExpr::sign, 0))?;
    class.define_method("is_duplicated", method!(RbExpr::is_duplicated, 0))?;
    class.define_method("over", method!(RbExpr::over, 1))?;
    class.define_method("_and", method!(RbExpr::_and, 1))?;
    class.define_method("_xor", method!(RbExpr::_xor, 1))?;
    class.define_method("_or", method!(RbExpr::_or, 1))?;
    class.define_method("is_in", method!(RbExpr::is_in, 1))?;
    class.define_method("repeat_by", method!(RbExpr::repeat_by, 1))?;
    class.define_method("pow", method!(RbExpr::pow, 1))?;
    class.define_method("cumsum", method!(RbExpr::cumsum, 1))?;
    class.define_method("cummax", method!(RbExpr::cummax, 1))?;
    class.define_method("cummin", method!(RbExpr::cummin, 1))?;
    class.define_method("cumprod", method!(RbExpr::cumprod, 1))?;
    class.define_method("product", method!(RbExpr::product, 0))?;
    class.define_method("shrink_dtype", method!(RbExpr::shrink_dtype, 0))?;
    class.define_method("str_parse_date", method!(RbExpr::str_parse_date, 4))?;
    class.define_method("str_parse_datetime", method!(RbExpr::str_parse_datetime, 5))?;
    class.define_method("str_parse_time", method!(RbExpr::str_parse_time, 4))?;
    class.define_method("str_strip", method!(RbExpr::str_strip, 1))?;
    class.define_method("str_rstrip", method!(RbExpr::str_rstrip, 1))?;
    class.define_method("str_lstrip", method!(RbExpr::str_lstrip, 1))?;
    class.define_method("str_slice", method!(RbExpr::str_slice, 2))?;
    class.define_method("str_to_uppercase", method!(RbExpr::str_to_uppercase, 0))?;
    class.define_method("str_to_lowercase", method!(RbExpr::str_to_lowercase, 0))?;
    class.define_method("str_lengths", method!(RbExpr::str_lengths, 0))?;
    class.define_method("str_n_chars", method!(RbExpr::str_n_chars, 0))?;
    class.define_method("str_replace", method!(RbExpr::str_replace, 3))?;
    class.define_method("str_replace_all", method!(RbExpr::str_replace_all, 3))?;
    class.define_method("str_zfill", method!(RbExpr::str_zfill, 1))?;
    class.define_method("str_ljust", method!(RbExpr::str_ljust, 2))?;
    class.define_method("str_rjust", method!(RbExpr::str_rjust, 2))?;
    class.define_method("str_contains", method!(RbExpr::str_contains, 2))?;
    class.define_method("str_ends_with", method!(RbExpr::str_ends_with, 1))?;
    class.define_method("str_starts_with", method!(RbExpr::str_starts_with, 1))?;
    class.define_method("str_hex_encode", method!(RbExpr::str_hex_encode, 0))?;
    class.define_method("str_hex_decode", method!(RbExpr::str_hex_decode, 1))?;
    class.define_method("str_base64_encode", method!(RbExpr::str_base64_encode, 0))?;
    class.define_method("str_base64_decode", method!(RbExpr::str_base64_decode, 1))?;
    class.define_method(
        "str_json_path_match",
        method!(RbExpr::str_json_path_match, 1),
    )?;
    class.define_method("str_extract", method!(RbExpr::str_extract, 2))?;
    class.define_method("str_extract_all", method!(RbExpr::str_extract_all, 1))?;
    class.define_method("count_match", method!(RbExpr::count_match, 1))?;
    class.define_method("strftime", method!(RbExpr::strftime, 1))?;
    class.define_method("str_split", method!(RbExpr::str_split, 1))?;
    class.define_method(
        "str_split_inclusive",
        method!(RbExpr::str_split_inclusive, 1),
    )?;
    class.define_method("str_split_exact", method!(RbExpr::str_split_exact, 2))?;
    class.define_method(
        "str_split_exact_inclusive",
        method!(RbExpr::str_split_exact_inclusive, 2),
    )?;
    class.define_method("str_splitn", method!(RbExpr::str_splitn, 2))?;
    class.define_method("arr_lengths", method!(RbExpr::arr_lengths, 0))?;
    class.define_method("arr_contains", method!(RbExpr::arr_contains, 1))?;
    class.define_method("year", method!(RbExpr::year, 0))?;
    class.define_method("iso_year", method!(RbExpr::iso_year, 0))?;
    class.define_method("quarter", method!(RbExpr::quarter, 0))?;
    class.define_method("month", method!(RbExpr::month, 0))?;
    class.define_method("week", method!(RbExpr::week, 0))?;
    class.define_method("weekday", method!(RbExpr::weekday, 0))?;
    class.define_method("day", method!(RbExpr::day, 0))?;
    class.define_method("ordinal_day", method!(RbExpr::ordinal_day, 0))?;
    class.define_method("hour", method!(RbExpr::hour, 0))?;
    class.define_method("minute", method!(RbExpr::minute, 0))?;
    class.define_method("second", method!(RbExpr::second, 0))?;
    class.define_method("millisecond", method!(RbExpr::millisecond, 0))?;
    class.define_method("microsecond", method!(RbExpr::microsecond, 0))?;
    class.define_method("nanosecond", method!(RbExpr::nanosecond, 0))?;
    class.define_method("duration_days", method!(RbExpr::duration_days, 0))?;
    class.define_method("duration_hours", method!(RbExpr::duration_hours, 0))?;
    class.define_method("duration_minutes", method!(RbExpr::duration_minutes, 0))?;
    class.define_method("duration_seconds", method!(RbExpr::duration_seconds, 0))?;
    class.define_method(
        "duration_nanoseconds",
        method!(RbExpr::duration_nanoseconds, 0),
    )?;
    class.define_method(
        "duration_microseconds",
        method!(RbExpr::duration_microseconds, 0),
    )?;
    class.define_method(
        "duration_milliseconds",
        method!(RbExpr::duration_milliseconds, 0),
    )?;
    class.define_method("timestamp", method!(RbExpr::timestamp, 1))?;
    class.define_method("dt_offset_by", method!(RbExpr::dt_offset_by, 1))?;
    class.define_method("dt_epoch_seconds", method!(RbExpr::dt_epoch_seconds, 0))?;
    class.define_method("dt_with_time_unit", method!(RbExpr::dt_with_time_unit, 1))?;
    class.define_method("dt_with_time_zone", method!(RbExpr::dt_with_time_zone, 1))?;
    class.define_method("dt_cast_time_unit", method!(RbExpr::dt_cast_time_unit, 1))?;
    class.define_method("dt_cast_time_zone", method!(RbExpr::dt_cast_time_zone, 1))?;
    class.define_method("dt_tz_localize", method!(RbExpr::dt_tz_localize, 1))?;
    class.define_method("dt_truncate", method!(RbExpr::dt_truncate, 2))?;
    class.define_method("dt_round", method!(RbExpr::dt_round, 2))?;
    class.define_method("map", method!(RbExpr::map, 3))?;
    class.define_method("dot", method!(RbExpr::dot, 1))?;
    class.define_method("reinterpret", method!(RbExpr::reinterpret, 1))?;
    class.define_method("mode", method!(RbExpr::mode, 0))?;
    class.define_method("keep_name", method!(RbExpr::keep_name, 0))?;
    class.define_method("prefix", method!(RbExpr::prefix, 1))?;
    class.define_method("suffix", method!(RbExpr::suffix, 1))?;
    class.define_method("map_alias", method!(RbExpr::map_alias, 1))?;
    class.define_method("exclude", method!(RbExpr::exclude, 1))?;
    class.define_method("interpolate", method!(RbExpr::interpolate, 1))?;
    class.define_method("rolling_sum", method!(RbExpr::rolling_sum, 6))?;
    class.define_method("rolling_min", method!(RbExpr::rolling_min, 6))?;
    class.define_method("rolling_max", method!(RbExpr::rolling_max, 6))?;
    class.define_method("rolling_mean", method!(RbExpr::rolling_mean, 6))?;
    class.define_method("rolling_std", method!(RbExpr::rolling_std, 6))?;
    class.define_method("rolling_var", method!(RbExpr::rolling_var, 6))?;
    class.define_method("rolling_median", method!(RbExpr::rolling_median, 6))?;
    class.define_method("rolling_quantile", method!(RbExpr::rolling_quantile, 8))?;
    class.define_method("rolling_skew", method!(RbExpr::rolling_skew, 2))?;
    class.define_method("lower_bound", method!(RbExpr::lower_bound, 0))?;
    class.define_method("upper_bound", method!(RbExpr::upper_bound, 0))?;
    class.define_method("lst_max", method!(RbExpr::lst_max, 0))?;
    class.define_method("lst_min", method!(RbExpr::lst_min, 0))?;
    class.define_method("lst_sum", method!(RbExpr::lst_sum, 0))?;
    class.define_method("lst_mean", method!(RbExpr::lst_mean, 0))?;
    class.define_method("lst_sort", method!(RbExpr::lst_sort, 1))?;
    class.define_method("lst_reverse", method!(RbExpr::lst_reverse, 0))?;
    class.define_method("lst_unique", method!(RbExpr::lst_unique, 0))?;
    class.define_method("lst_get", method!(RbExpr::lst_get, 1))?;
    class.define_method("lst_join", method!(RbExpr::lst_join, 1))?;
    class.define_method("lst_arg_min", method!(RbExpr::lst_arg_min, 0))?;
    class.define_method("lst_arg_max", method!(RbExpr::lst_arg_max, 0))?;
    class.define_method("lst_diff", method!(RbExpr::lst_diff, 2))?;
    class.define_method("lst_shift", method!(RbExpr::lst_shift, 1))?;
    class.define_method("lst_slice", method!(RbExpr::lst_slice, 2))?;
    class.define_method("lst_eval", method!(RbExpr::lst_eval, 2))?;
    class.define_method("cumulative_eval", method!(RbExpr::cumulative_eval, 3))?;
    class.define_method("lst_to_struct", method!(RbExpr::lst_to_struct, 3))?;
    class.define_method("rank", method!(RbExpr::rank, 2))?;
    class.define_method("diff", method!(RbExpr::diff, 2))?;
    class.define_method("pct_change", method!(RbExpr::pct_change, 1))?;
    class.define_method("skew", method!(RbExpr::skew, 1))?;
    class.define_method("kurtosis", method!(RbExpr::kurtosis, 2))?;
    class.define_method("str_concat", method!(RbExpr::str_concat, 1))?;
    class.define_method("cat_set_ordering", method!(RbExpr::cat_set_ordering, 1))?;
    class.define_method("reshape", method!(RbExpr::reshape, 1))?;
    class.define_method("cumcount", method!(RbExpr::cumcount, 1))?;
    class.define_method("to_physical", method!(RbExpr::to_physical, 0))?;
    class.define_method("shuffle", method!(RbExpr::shuffle, 1))?;
    class.define_method("sample_n", method!(RbExpr::sample_n, 4))?;
    class.define_method("sample_frac", method!(RbExpr::sample_frac, 4))?;
    class.define_method("ewm_mean", method!(RbExpr::ewm_mean, 3))?;
    class.define_method("ewm_std", method!(RbExpr::ewm_std, 4))?;
    class.define_method("ewm_var", method!(RbExpr::ewm_var, 4))?;
    class.define_method("extend_constant", method!(RbExpr::extend_constant, 2))?;
    class.define_method("any", method!(RbExpr::any, 0))?;
    class.define_method("all", method!(RbExpr::all, 0))?;
    class.define_method(
        "struct_field_by_name",
        method!(RbExpr::struct_field_by_name, 1),
    )?;
    class.define_method(
        "struct_field_by_index",
        method!(RbExpr::struct_field_by_index, 1),
    )?;
    class.define_method(
        "struct_rename_fields",
        method!(RbExpr::struct_rename_fields, 1),
    )?;
    class.define_method("log", method!(RbExpr::log, 1))?;
    class.define_method("exp", method!(RbExpr::exp, 0))?;
    class.define_method("entropy", method!(RbExpr::entropy, 2))?;
    class.define_method("_hash", method!(RbExpr::hash, 4))?;

    // meta
    class.define_method("meta_pop", method!(RbExpr::meta_pop, 0))?;
    class.define_method("meta_eq", method!(RbExpr::meta_eq, 1))?;
    class.define_method("meta_roots", method!(RbExpr::meta_roots, 0))?;
    class.define_method("meta_output_name", method!(RbExpr::meta_output_name, 0))?;
    class.define_method("meta_undo_aliases", method!(RbExpr::meta_undo_aliases, 0))?;

    // maybe add to different class
    class.define_singleton_method("col", function!(crate::lazy::dsl::col, 1))?;
    class.define_singleton_method("count", function!(crate::lazy::dsl::count, 0))?;
    class.define_singleton_method("first", function!(crate::lazy::dsl::first, 0))?;
    class.define_singleton_method("last", function!(crate::lazy::dsl::last, 0))?;
    class.define_singleton_method("cols", function!(crate::lazy::dsl::cols, 1))?;
    class.define_singleton_method("fold", function!(crate::lazy::dsl::fold, 3))?;
    class.define_singleton_method("cumfold", function!(crate::lazy::dsl::cumfold, 4))?;
    class.define_singleton_method("lit", function!(crate::lazy::dsl::lit, 1))?;
    class.define_singleton_method("arange", function!(crate::lazy::dsl::arange, 3))?;
    class.define_singleton_method("repeat", function!(crate::lazy::dsl::repeat, 2))?;
    class.define_singleton_method("pearson_corr", function!(crate::lazy::dsl::pearson_corr, 3))?;
    class.define_singleton_method(
        "spearman_rank_corr",
        function!(crate::lazy::dsl::spearman_rank_corr, 4),
    )?;
    class.define_singleton_method("cov", function!(crate::lazy::dsl::cov, 2))?;
    class.define_singleton_method("argsort_by", function!(crate::lazy::dsl::argsort_by, 2))?;
    class.define_singleton_method("when", function!(crate::lazy::dsl::when, 1))?;
    class.define_singleton_method("concat_str", function!(crate::lazy::dsl::concat_str, 2))?;
    class.define_singleton_method("concat_lst", function!(crate::lazy::dsl::concat_lst, 1))?;

    let class = module.define_class("RbLazyFrame", Default::default())?;
    class.define_singleton_method("read_json", function!(RbLazyFrame::read_json, 1))?;
    class.define_singleton_method(
        "new_from_ndjson",
        function!(RbLazyFrame::new_from_ndjson, 7),
    )?;
    class.define_singleton_method("new_from_csv", function!(RbLazyFrame::new_from_csv, -1))?;
    class.define_singleton_method(
        "new_from_parquet",
        function!(RbLazyFrame::new_from_parquet, 7),
    )?;
    class.define_singleton_method("new_from_ipc", function!(RbLazyFrame::new_from_ipc, 6))?;
    class.define_method("write_json", method!(RbLazyFrame::write_json, 1))?;
    class.define_method("describe_plan", method!(RbLazyFrame::describe_plan, 0))?;
    class.define_method(
        "describe_optimized_plan",
        method!(RbLazyFrame::describe_optimized_plan, 0),
    )?;
    class.define_method(
        "optimization_toggle",
        method!(RbLazyFrame::optimization_toggle, 7),
    )?;
    class.define_method("sort", method!(RbLazyFrame::sort, 3))?;
    class.define_method("sort_by_exprs", method!(RbLazyFrame::sort_by_exprs, 3))?;
    class.define_method("cache", method!(RbLazyFrame::cache, 0))?;
    class.define_method("collect", method!(RbLazyFrame::collect, 0))?;
    class.define_method("fetch", method!(RbLazyFrame::fetch, 1))?;
    class.define_method("filter", method!(RbLazyFrame::filter, 1))?;
    class.define_method("select", method!(RbLazyFrame::select, 1))?;
    class.define_method("groupby", method!(RbLazyFrame::groupby, 2))?;
    class.define_method("groupby_rolling", method!(RbLazyFrame::groupby_rolling, 5))?;
    class.define_method("groupby_dynamic", method!(RbLazyFrame::groupby_dynamic, 9))?;
    class.define_method("with_context", method!(RbLazyFrame::with_context, 1))?;
    class.define_method("join_asof", method!(RbLazyFrame::join_asof, 11))?;
    class.define_method("join", method!(RbLazyFrame::join, 7))?;
    class.define_method("with_columns", method!(RbLazyFrame::with_columns, 1))?;
    class.define_method("rename", method!(RbLazyFrame::rename, 2))?;
    class.define_method("reverse", method!(RbLazyFrame::reverse, 0))?;
    class.define_method("shift", method!(RbLazyFrame::shift, 1))?;
    class.define_method("shift_and_fill", method!(RbLazyFrame::shift_and_fill, 2))?;
    class.define_method("fill_nan", method!(RbLazyFrame::fill_nan, 1))?;
    class.define_method("min", method!(RbLazyFrame::min, 0))?;
    class.define_method("max", method!(RbLazyFrame::max, 0))?;
    class.define_method("sum", method!(RbLazyFrame::sum, 0))?;
    class.define_method("mean", method!(RbLazyFrame::mean, 0))?;
    class.define_method("std", method!(RbLazyFrame::std, 1))?;
    class.define_method("var", method!(RbLazyFrame::var, 1))?;
    class.define_method("median", method!(RbLazyFrame::median, 0))?;
    class.define_method("quantile", method!(RbLazyFrame::quantile, 2))?;
    class.define_method("explode", method!(RbLazyFrame::explode, 1))?;
    class.define_method("unique", method!(RbLazyFrame::unique, 3))?;
    class.define_method("drop_nulls", method!(RbLazyFrame::drop_nulls, 1))?;
    class.define_method("slice", method!(RbLazyFrame::slice, 2))?;
    class.define_method("tail", method!(RbLazyFrame::tail, 1))?;
    class.define_method("melt", method!(RbLazyFrame::melt, 4))?;
    class.define_method("with_row_count", method!(RbLazyFrame::with_row_count, 2))?;
    class.define_method("drop_columns", method!(RbLazyFrame::drop_columns, 1))?;
    class.define_method("_clone", method!(RbLazyFrame::clone, 0))?;
    class.define_method("columns", method!(RbLazyFrame::columns, 0))?;
    class.define_method("dtypes", method!(RbLazyFrame::dtypes, 0))?;
    class.define_method("schema", method!(RbLazyFrame::schema, 0))?;
    class.define_method("unnest", method!(RbLazyFrame::unnest, 1))?;
    class.define_method("width", method!(RbLazyFrame::width, 0))?;

    let class = module.define_class("RbLazyGroupBy", Default::default())?;
    class.define_method("agg", method!(RbLazyGroupBy::agg, 1))?;
    class.define_method("head", method!(RbLazyGroupBy::head, 1))?;
    class.define_method("tail", method!(RbLazyGroupBy::tail, 1))?;

    let class = module.define_class("RbSeries", Default::default())?;
    class.define_singleton_method("new_opt_bool", function!(RbSeries::new_opt_bool, 3))?;
    class.define_singleton_method("new_opt_u8", function!(RbSeries::new_opt_u8, 3))?;
    class.define_singleton_method("new_opt_u16", function!(RbSeries::new_opt_u16, 3))?;
    class.define_singleton_method("new_opt_u32", function!(RbSeries::new_opt_u32, 3))?;
    class.define_singleton_method("new_opt_u64", function!(RbSeries::new_opt_u64, 3))?;
    class.define_singleton_method("new_opt_i8", function!(RbSeries::new_opt_i8, 3))?;
    class.define_singleton_method("new_opt_i16", function!(RbSeries::new_opt_i16, 3))?;
    class.define_singleton_method("new_opt_i32", function!(RbSeries::new_opt_i32, 3))?;
    class.define_singleton_method("new_opt_i64", function!(RbSeries::new_opt_i64, 3))?;
    class.define_singleton_method("new_opt_f32", function!(RbSeries::new_opt_f32, 3))?;
    class.define_singleton_method("new_opt_f64", function!(RbSeries::new_opt_f64, 3))?;
    class.define_singleton_method("new_str", function!(RbSeries::new_str, 3))?;
    class.define_singleton_method("new_object", function!(RbSeries::new_object, 3))?;
    class.define_singleton_method("new_list", function!(RbSeries::new_list, 3))?;
    class.define_singleton_method("new_opt_date", function!(RbSeries::new_opt_date, 3))?;
    class.define_singleton_method("new_opt_datetime", function!(RbSeries::new_opt_datetime, 3))?;
    class.define_method("is_sorted_flag", method!(RbSeries::is_sorted_flag, 0))?;
    class.define_method(
        "is_sorted_reverse_flag",
        method!(RbSeries::is_sorted_reverse_flag, 0),
    )?;
    class.define_method("estimated_size", method!(RbSeries::estimated_size, 0))?;
    class.define_method("get_fmt", method!(RbSeries::get_fmt, 2))?;
    class.define_method("rechunk", method!(RbSeries::rechunk, 1))?;
    class.define_method("get_idx", method!(RbSeries::get_idx, 1))?;
    class.define_method("bitand", method!(RbSeries::bitand, 1))?;
    class.define_method("bitor", method!(RbSeries::bitor, 1))?;
    class.define_method("bitxor", method!(RbSeries::bitxor, 1))?;
    class.define_method("chunk_lengths", method!(RbSeries::chunk_lengths, 0))?;
    class.define_method("name", method!(RbSeries::name, 0))?;
    class.define_method("rename", method!(RbSeries::rename, 1))?;
    class.define_method("dtype", method!(RbSeries::dtype, 0))?;
    class.define_method("inner_dtype", method!(RbSeries::inner_dtype, 0))?;
    class.define_method("set_sorted", method!(RbSeries::set_sorted, 1))?;
    class.define_method("mean", method!(RbSeries::mean, 0))?;
    class.define_method("max", method!(RbSeries::max, 0))?;
    class.define_method("min", method!(RbSeries::min, 0))?;
    class.define_method("sum", method!(RbSeries::sum, 0))?;
    class.define_method("n_chunks", method!(RbSeries::n_chunks, 0))?;
    class.define_method("append", method!(RbSeries::append, 1))?;
    class.define_method("extend", method!(RbSeries::extend, 1))?;
    class.define_method("new_from_index", method!(RbSeries::new_from_index, 2))?;
    class.define_method("filter", method!(RbSeries::filter, 1))?;
    class.define_method("add", method!(RbSeries::add, 1))?;
    class.define_method("sub", method!(RbSeries::sub, 1))?;
    class.define_method("mul", method!(RbSeries::mul, 1))?;
    class.define_method("div", method!(RbSeries::div, 1))?;
    class.define_method("rem", method!(RbSeries::rem, 1))?;
    class.define_method("sort", method!(RbSeries::sort, 1))?;
    class.define_method("value_counts", method!(RbSeries::value_counts, 1))?;
    class.define_method("arg_min", method!(RbSeries::arg_min, 0))?;
    class.define_method("arg_max", method!(RbSeries::arg_max, 0))?;
    class.define_method("take_with_series", method!(RbSeries::take_with_series, 1))?;
    class.define_method("null_count", method!(RbSeries::null_count, 0))?;
    class.define_method("has_validity", method!(RbSeries::has_validity, 0))?;
    class.define_method("sample_n", method!(RbSeries::sample_n, 4))?;
    class.define_method("sample_frac", method!(RbSeries::sample_frac, 4))?;
    class.define_method("series_equal", method!(RbSeries::series_equal, 3))?;
    class.define_method("eq", method!(RbSeries::eq, 1))?;
    class.define_method("neq", method!(RbSeries::neq, 1))?;
    class.define_method("gt", method!(RbSeries::gt, 1))?;
    class.define_method("gt_eq", method!(RbSeries::gt_eq, 1))?;
    class.define_method("lt", method!(RbSeries::lt, 1))?;
    class.define_method("lt_eq", method!(RbSeries::lt_eq, 1))?;
    class.define_method("not", method!(RbSeries::not, 0))?;
    class.define_method("to_s", method!(RbSeries::to_s, 0))?;
    class.define_method("len", method!(RbSeries::len, 0))?;
    class.define_method("to_a", method!(RbSeries::to_a, 0))?;
    class.define_method("median", method!(RbSeries::median, 0))?;
    class.define_method("quantile", method!(RbSeries::quantile, 2))?;
    class.define_method("_clone", method!(RbSeries::clone, 0))?;
    class.define_method("apply_lambda", method!(RbSeries::apply_lambda, 3))?;
    class.define_method("zip_with", method!(RbSeries::zip_with, 2))?;
    class.define_method("to_dummies", method!(RbSeries::to_dummies, 0))?;
    class.define_method("peak_max", method!(RbSeries::peak_max, 0))?;
    class.define_method("peak_min", method!(RbSeries::peak_min, 0))?;
    class.define_method("n_unique", method!(RbSeries::n_unique, 0))?;
    class.define_method("floor", method!(RbSeries::floor, 0))?;
    class.define_method("shrink_to_fit", method!(RbSeries::shrink_to_fit, 0))?;
    class.define_method("dot", method!(RbSeries::dot, 1))?;
    class.define_method("skew", method!(RbSeries::skew, 1))?;
    class.define_method("kurtosis", method!(RbSeries::kurtosis, 2))?;
    class.define_method("cast", method!(RbSeries::cast, 2))?;
    class.define_method("time_unit", method!(RbSeries::time_unit, 0))?;
    class.define_method("set_at_idx", method!(RbSeries::set_at_idx, 2))?;

    // set
    // class.define_method("set_with_mask_str", method!(RbSeries::set_with_mask_str, 2))?;
    class.define_method("set_with_mask_f64", method!(RbSeries::set_with_mask_f64, 2))?;
    class.define_method("set_with_mask_f32", method!(RbSeries::set_with_mask_f32, 2))?;
    class.define_method("set_with_mask_u8", method!(RbSeries::set_with_mask_u8, 2))?;
    class.define_method("set_with_mask_u16", method!(RbSeries::set_with_mask_u16, 2))?;
    class.define_method("set_with_mask_u32", method!(RbSeries::set_with_mask_u32, 2))?;
    class.define_method("set_with_mask_u64", method!(RbSeries::set_with_mask_u64, 2))?;
    class.define_method("set_with_mask_i8", method!(RbSeries::set_with_mask_i8, 2))?;
    class.define_method("set_with_mask_i16", method!(RbSeries::set_with_mask_i16, 2))?;
    class.define_method("set_with_mask_i32", method!(RbSeries::set_with_mask_i32, 2))?;
    class.define_method("set_with_mask_i64", method!(RbSeries::set_with_mask_i64, 2))?;
    class.define_method(
        "set_with_mask_bool",
        method!(RbSeries::set_with_mask_bool, 2),
    )?;

    // arithmetic
    class.define_method("add_u8", method!(RbSeries::add_u8, 1))?;
    class.define_method("add_u16", method!(RbSeries::add_u16, 1))?;
    class.define_method("add_u32", method!(RbSeries::add_u32, 1))?;
    class.define_method("add_u64", method!(RbSeries::add_u64, 1))?;
    class.define_method("add_i8", method!(RbSeries::add_i8, 1))?;
    class.define_method("add_i16", method!(RbSeries::add_i16, 1))?;
    class.define_method("add_i32", method!(RbSeries::add_i32, 1))?;
    class.define_method("add_i64", method!(RbSeries::add_i64, 1))?;
    class.define_method("add_datetime", method!(RbSeries::add_datetime, 1))?;
    class.define_method("add_duration", method!(RbSeries::add_duration, 1))?;
    class.define_method("add_f32", method!(RbSeries::add_f32, 1))?;
    class.define_method("add_f64", method!(RbSeries::add_f64, 1))?;
    class.define_method("sub_u8", method!(RbSeries::sub_u8, 1))?;
    class.define_method("sub_u16", method!(RbSeries::sub_u16, 1))?;
    class.define_method("sub_u32", method!(RbSeries::sub_u32, 1))?;
    class.define_method("sub_u64", method!(RbSeries::sub_u64, 1))?;
    class.define_method("sub_i8", method!(RbSeries::sub_i8, 1))?;
    class.define_method("sub_i16", method!(RbSeries::sub_i16, 1))?;
    class.define_method("sub_i32", method!(RbSeries::sub_i32, 1))?;
    class.define_method("sub_i64", method!(RbSeries::sub_i64, 1))?;
    class.define_method("sub_datetime", method!(RbSeries::sub_datetime, 1))?;
    class.define_method("sub_duration", method!(RbSeries::sub_duration, 1))?;
    class.define_method("sub_f32", method!(RbSeries::sub_f32, 1))?;
    class.define_method("sub_f64", method!(RbSeries::sub_f64, 1))?;
    class.define_method("div_u8", method!(RbSeries::div_u8, 1))?;
    class.define_method("div_u16", method!(RbSeries::div_u16, 1))?;
    class.define_method("div_u32", method!(RbSeries::div_u32, 1))?;
    class.define_method("div_u64", method!(RbSeries::div_u64, 1))?;
    class.define_method("div_i8", method!(RbSeries::div_i8, 1))?;
    class.define_method("div_i16", method!(RbSeries::div_i16, 1))?;
    class.define_method("div_i32", method!(RbSeries::div_i32, 1))?;
    class.define_method("div_i64", method!(RbSeries::div_i64, 1))?;
    class.define_method("div_f32", method!(RbSeries::div_f32, 1))?;
    class.define_method("div_f64", method!(RbSeries::div_f64, 1))?;
    class.define_method("mul_u8", method!(RbSeries::mul_u8, 1))?;
    class.define_method("mul_u16", method!(RbSeries::mul_u16, 1))?;
    class.define_method("mul_u32", method!(RbSeries::mul_u32, 1))?;
    class.define_method("mul_u64", method!(RbSeries::mul_u64, 1))?;
    class.define_method("mul_i8", method!(RbSeries::mul_i8, 1))?;
    class.define_method("mul_i16", method!(RbSeries::mul_i16, 1))?;
    class.define_method("mul_i32", method!(RbSeries::mul_i32, 1))?;
    class.define_method("mul_i64", method!(RbSeries::mul_i64, 1))?;
    class.define_method("mul_f32", method!(RbSeries::mul_f32, 1))?;
    class.define_method("mul_f64", method!(RbSeries::mul_f64, 1))?;
    class.define_method("rem_u8", method!(RbSeries::rem_u8, 1))?;
    class.define_method("rem_u16", method!(RbSeries::rem_u16, 1))?;
    class.define_method("rem_u32", method!(RbSeries::rem_u32, 1))?;
    class.define_method("rem_u64", method!(RbSeries::rem_u64, 1))?;
    class.define_method("rem_i8", method!(RbSeries::rem_i8, 1))?;
    class.define_method("rem_i16", method!(RbSeries::rem_i16, 1))?;
    class.define_method("rem_i32", method!(RbSeries::rem_i32, 1))?;
    class.define_method("rem_i64", method!(RbSeries::rem_i64, 1))?;
    class.define_method("rem_f32", method!(RbSeries::rem_f32, 1))?;
    class.define_method("rem_f64", method!(RbSeries::rem_f64, 1))?;

    // eq
    class.define_method("eq_u8", method!(RbSeries::eq_u8, 1))?;
    class.define_method("eq_u16", method!(RbSeries::eq_u16, 1))?;
    class.define_method("eq_u32", method!(RbSeries::eq_u32, 1))?;
    class.define_method("eq_u64", method!(RbSeries::eq_u64, 1))?;
    class.define_method("eq_i8", method!(RbSeries::eq_i8, 1))?;
    class.define_method("eq_i16", method!(RbSeries::eq_i16, 1))?;
    class.define_method("eq_i32", method!(RbSeries::eq_i32, 1))?;
    class.define_method("eq_i64", method!(RbSeries::eq_i64, 1))?;
    class.define_method("eq_f32", method!(RbSeries::eq_f32, 1))?;
    class.define_method("eq_f64", method!(RbSeries::eq_f64, 1))?;
    // class.define_method("eq_str", method!(RbSeries::eq_str, 1))?;

    // neq
    class.define_method("neq_u8", method!(RbSeries::neq_u8, 1))?;
    class.define_method("neq_u16", method!(RbSeries::neq_u16, 1))?;
    class.define_method("neq_u32", method!(RbSeries::neq_u32, 1))?;
    class.define_method("neq_u64", method!(RbSeries::neq_u64, 1))?;
    class.define_method("neq_i8", method!(RbSeries::neq_i8, 1))?;
    class.define_method("neq_i16", method!(RbSeries::neq_i16, 1))?;
    class.define_method("neq_i32", method!(RbSeries::neq_i32, 1))?;
    class.define_method("neq_i64", method!(RbSeries::neq_i64, 1))?;
    class.define_method("neq_f32", method!(RbSeries::neq_f32, 1))?;
    class.define_method("neq_f64", method!(RbSeries::neq_f64, 1))?;
    // class.define_method("neq_str", method!(RbSeries::neq_str, 1))?;

    // gt
    class.define_method("gt_u8", method!(RbSeries::gt_u8, 1))?;
    class.define_method("gt_u16", method!(RbSeries::gt_u16, 1))?;
    class.define_method("gt_u32", method!(RbSeries::gt_u32, 1))?;
    class.define_method("gt_u64", method!(RbSeries::gt_u64, 1))?;
    class.define_method("gt_i8", method!(RbSeries::gt_i8, 1))?;
    class.define_method("gt_i16", method!(RbSeries::gt_i16, 1))?;
    class.define_method("gt_i32", method!(RbSeries::gt_i32, 1))?;
    class.define_method("gt_i64", method!(RbSeries::gt_i64, 1))?;
    class.define_method("gt_f32", method!(RbSeries::gt_f32, 1))?;
    class.define_method("gt_f64", method!(RbSeries::gt_f64, 1))?;
    // class.define_method("gt_str", method!(RbSeries::gt_str, 1))?;

    // gt_eq
    class.define_method("gt_eq_u8", method!(RbSeries::gt_eq_u8, 1))?;
    class.define_method("gt_eq_u16", method!(RbSeries::gt_eq_u16, 1))?;
    class.define_method("gt_eq_u32", method!(RbSeries::gt_eq_u32, 1))?;
    class.define_method("gt_eq_u64", method!(RbSeries::gt_eq_u64, 1))?;
    class.define_method("gt_eq_i8", method!(RbSeries::gt_eq_i8, 1))?;
    class.define_method("gt_eq_i16", method!(RbSeries::gt_eq_i16, 1))?;
    class.define_method("gt_eq_i32", method!(RbSeries::gt_eq_i32, 1))?;
    class.define_method("gt_eq_i64", method!(RbSeries::gt_eq_i64, 1))?;
    class.define_method("gt_eq_f32", method!(RbSeries::gt_eq_f32, 1))?;
    class.define_method("gt_eq_f64", method!(RbSeries::gt_eq_f64, 1))?;
    // class.define_method("gt_eq_str", method!(RbSeries::gt_eq_str, 1))?;

    // lt
    class.define_method("lt_u8", method!(RbSeries::lt_u8, 1))?;
    class.define_method("lt_u16", method!(RbSeries::lt_u16, 1))?;
    class.define_method("lt_u32", method!(RbSeries::lt_u32, 1))?;
    class.define_method("lt_u64", method!(RbSeries::lt_u64, 1))?;
    class.define_method("lt_i8", method!(RbSeries::lt_i8, 1))?;
    class.define_method("lt_i16", method!(RbSeries::lt_i16, 1))?;
    class.define_method("lt_i32", method!(RbSeries::lt_i32, 1))?;
    class.define_method("lt_i64", method!(RbSeries::lt_i64, 1))?;
    class.define_method("lt_f32", method!(RbSeries::lt_f32, 1))?;
    class.define_method("lt_f64", method!(RbSeries::lt_f64, 1))?;
    // class.define_method("lt_str", method!(RbSeries::lt_str, 1))?;

    // lt_eq
    class.define_method("lt_eq_u8", method!(RbSeries::lt_eq_u8, 1))?;
    class.define_method("lt_eq_u16", method!(RbSeries::lt_eq_u16, 1))?;
    class.define_method("lt_eq_u32", method!(RbSeries::lt_eq_u32, 1))?;
    class.define_method("lt_eq_u64", method!(RbSeries::lt_eq_u64, 1))?;
    class.define_method("lt_eq_i8", method!(RbSeries::lt_eq_i8, 1))?;
    class.define_method("lt_eq_i16", method!(RbSeries::lt_eq_i16, 1))?;
    class.define_method("lt_eq_i32", method!(RbSeries::lt_eq_i32, 1))?;
    class.define_method("lt_eq_i64", method!(RbSeries::lt_eq_i64, 1))?;
    class.define_method("lt_eq_f32", method!(RbSeries::lt_eq_f32, 1))?;
    class.define_method("lt_eq_f64", method!(RbSeries::lt_eq_f64, 1))?;
    // class.define_method("lt_eq_str", method!(RbSeries::lt_eq_str, 1))?;

    let class = module.define_class("RbWhen", Default::default())?;
    class.define_method("_then", method!(RbWhen::then, 1))?;

    let class = module.define_class("RbWhenThen", Default::default())?;
    class.define_method("otherwise", method!(RbWhenThen::overwise, 1))?;

    Ok(())
}

fn dtype_cols(dtypes: RArray) -> RbResult<RbExpr> {
    let dtypes = dtypes
        .each()
        .map(|v| v?.try_convert::<Wrap<DataType>>())
        .collect::<RbResult<Vec<Wrap<DataType>>>>()?;
    let dtypes = vec_extract_wrapped(dtypes);
    Ok(crate::lazy::dsl::dtype_cols(dtypes))
}

#[allow(clippy::too_many_arguments)]
fn rb_duration(
    days: Option<&RbExpr>,
    seconds: Option<&RbExpr>,
    nanoseconds: Option<&RbExpr>,
    microseconds: Option<&RbExpr>,
    milliseconds: Option<&RbExpr>,
    minutes: Option<&RbExpr>,
    hours: Option<&RbExpr>,
    weeks: Option<&RbExpr>,
) -> RbExpr {
    let args = DurationArgs {
        days: days.map(|e| e.inner.clone()),
        seconds: seconds.map(|e| e.inner.clone()),
        nanoseconds: nanoseconds.map(|e| e.inner.clone()),
        microseconds: microseconds.map(|e| e.inner.clone()),
        milliseconds: milliseconds.map(|e| e.inner.clone()),
        minutes: minutes.map(|e| e.inner.clone()),
        hours: hours.map(|e| e.inner.clone()),
        weeks: weeks.map(|e| e.inner.clone()),
    };

    polars::lazy::dsl::duration(args).into()
}

fn concat_df(seq: RArray) -> RbResult<RbDataFrame> {
    let mut iter = seq.each();
    let first = iter.next().unwrap()?;

    let first_rdf = get_df(first)?;
    let identity_df = first_rdf.slice(0, 0);

    let mut rdfs: Vec<PolarsResult<DataFrame>> = vec![Ok(first_rdf)];

    for item in iter {
        let rdf = get_df(item?)?;
        rdfs.push(Ok(rdf));
    }

    let identity = Ok(identity_df);

    let df = rdfs
        .into_iter()
        .fold(identity, |acc: PolarsResult<DataFrame>, df| {
            let mut acc = acc?;
            acc.vstack_mut(&df?)?;
            Ok(acc)
        })
        .map_err(RbPolarsErr::from)?;

    Ok(df.into())
}

fn concat_lf(lfs: Value, rechunk: bool, parallel: bool) -> RbResult<RbLazyFrame> {
    let (seq, len) = get_rbseq(lfs)?;
    let mut lfs = Vec::with_capacity(len);

    for res in seq.each() {
        let item = res?;
        let lf = get_lf(item)?;
        lfs.push(lf);
    }

    let lf = polars::lazy::dsl::concat(lfs, rechunk, parallel).map_err(RbPolarsErr::from)?;
    Ok(lf.into())
}

fn rb_diag_concat_df(seq: RArray) -> RbResult<RbDataFrame> {
    let mut dfs = Vec::new();
    for item in seq.each() {
        dfs.push(get_df(item?)?);
    }
    let df = diag_concat_df(&dfs).map_err(RbPolarsErr::from)?;
    Ok(df.into())
}

fn rb_hor_concat_df(seq: RArray) -> RbResult<RbDataFrame> {
    let mut dfs = Vec::new();
    for item in seq.each() {
        dfs.push(get_df(item?)?);
    }
    let df = hor_concat_df(&dfs).map_err(RbPolarsErr::from)?;
    Ok(df.into())
}

fn concat_series(seq: RArray) -> RbResult<RbSeries> {
    let mut iter = seq.each();
    let first = iter.next().unwrap()?;

    let mut s = get_series(first)?;

    for res in iter {
        let item = res?;
        let item = get_series(item)?;
        s.append(&item).map_err(RbPolarsErr::from)?;
    }
    Ok(s.into())
}

fn ipc_schema(rb_f: Value) -> RbResult<Value> {
    use polars::export::arrow::io::ipc::read::read_file_metadata;
    let mut r = get_file_like(rb_f, false)?;
    let metadata = read_file_metadata(&mut r).map_err(RbPolarsErr::arrow)?;

    let dict = RHash::new();
    for field in metadata.schema.fields {
        let dt: Wrap<DataType> = Wrap((&field.data_type).into());
        dict.aset(field.name, dt)?;
    }
    Ok(dict.into())
}

fn parquet_schema(rb_f: Value) -> RbResult<Value> {
    use polars::export::arrow::io::parquet::read::{infer_schema, read_metadata};

    let mut r = get_file_like(rb_f, false)?;
    let metadata = read_metadata(&mut r).map_err(RbPolarsErr::arrow)?;
    let arrow_schema = infer_schema(&metadata).map_err(RbPolarsErr::arrow)?;

    let dict = RHash::new();
    for field in arrow_schema.fields {
        let dt: Wrap<DataType> = Wrap((&field.data_type).into());
        dict.aset(field.name, dt)?;
    }
    Ok(dict.into())
}

fn collect_all(lfs: RArray) -> RbResult<Vec<RbDataFrame>> {
    use polars_core::utils::rayon::prelude::*;

    let lfs = lfs
        .each()
        .map(|v| v?.try_convert::<&RbLazyFrame>())
        .collect::<RbResult<Vec<&RbLazyFrame>>>()?;

    polars_core::POOL.install(|| {
        lfs.par_iter()
            .map(|lf| {
                let df = lf.ldf.clone().collect()?;
                Ok(RbDataFrame::new(df))
            })
            .collect::<polars_core::error::PolarsResult<Vec<_>>>()
            .map_err(RbPolarsErr::from)
    })
}

fn rb_date_range(
    start: i64,
    stop: i64,
    every: String,
    closed: Wrap<ClosedWindow>,
    name: String,
    tu: Wrap<TimeUnit>,
    tz: Option<TimeZone>,
) -> RbSeries {
    polars::time::date_range_impl(
        &name,
        start,
        stop,
        Duration::parse(&every),
        closed.0,
        tu.0,
        tz.as_ref(),
    )
    .into_series()
    .into()
}

fn coalesce_exprs(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(polars::lazy::dsl::coalesce(&exprs).into())
}

fn sum_exprs(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(polars::lazy::dsl::sum_exprs(exprs).into())
}

fn as_struct(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(polars::lazy::dsl::as_struct(&exprs).into())
}

fn arg_where(condition: &RbExpr) -> RbExpr {
    polars::lazy::dsl::arg_where(condition.inner.clone()).into()
}
