mod batched_csv;
mod conversion;
mod dataframe;
mod error;
mod expr;
mod file;
mod functions;
mod lazyframe;
mod lazygroupby;
mod map;
mod object;
mod prelude;
pub(crate) mod rb_modules;
mod series;
mod sql;
mod utils;

use batched_csv::RbBatchedCsv;
use conversion::*;
use dataframe::RbDataFrame;
use error::{RbPolarsErr, RbTypeError, RbValueError};
use expr::rb_exprs_to_exprs;
use expr::RbExpr;
use functions::whenthen::{RbThen, RbWhen};
use lazyframe::RbLazyFrame;
use lazygroupby::RbLazyGroupBy;
use magnus::{define_module, function, method, prelude::*, Error, Ruby};
use series::RbSeries;
use sql::RbSQLContext;

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

#[magnus::init]
fn init(ruby: &Ruby) -> RbResult<()> {
    let module = define_module("Polars")?;
    module.define_singleton_method(
        "_dtype_cols",
        function!(crate::functions::lazy::dtype_cols2, 1),
    )?;
    module.define_singleton_method(
        "_concat_lf_diagonal",
        function!(crate::functions::lazy::concat_lf_diagonal, 4),
    )?;
    module.define_singleton_method(
        "_rb_duration",
        function!(crate::functions::lazy::duration, 9),
    )?;
    module.define_singleton_method(
        "_concat_df",
        function!(crate::functions::eager::concat_df, 1),
    )?;
    module.define_singleton_method(
        "_concat_lf",
        function!(crate::functions::lazy::concat_lf, 4),
    )?;
    module.define_singleton_method(
        "_concat_df_diagonal",
        function!(crate::functions::eager::concat_df_diagonal, 1),
    )?;
    module.define_singleton_method(
        "_concat_df_horizontal",
        function!(crate::functions::eager::concat_df_horizontal, 1),
    )?;
    module.define_singleton_method(
        "_concat_series",
        function!(crate::functions::eager::concat_series, 1),
    )?;
    module.define_singleton_method(
        "_ipc_schema",
        function!(crate::functions::io::read_ipc_schema, 1),
    )?;
    module.define_singleton_method(
        "_parquet_schema",
        function!(crate::functions::io::read_parquet_schema, 1),
    )?;
    module.define_singleton_method(
        "_collect_all",
        function!(crate::functions::lazy::collect_all, 1),
    )?;
    module.define_singleton_method(
        "_rb_date_range",
        function!(crate::functions::range::date_range, 6),
    )?;
    module.define_singleton_method(
        "_coalesce_exprs",
        function!(crate::functions::lazy::coalesce, 1),
    )?;
    module.define_singleton_method(
        "_all_horizontal",
        function!(crate::functions::aggregation::all_horizontal, 1),
    )?;
    module.define_singleton_method(
        "_any_horizontal",
        function!(crate::functions::aggregation::any_horizontal, 1),
    )?;
    module.define_singleton_method(
        "_max_horizontal",
        function!(crate::functions::aggregation::max_horizontal, 1),
    )?;
    module.define_singleton_method(
        "_min_horizontal",
        function!(crate::functions::aggregation::min_horizontal, 1),
    )?;
    module.define_singleton_method(
        "_sum_horizontal",
        function!(crate::functions::aggregation::sum_horizontal, 1),
    )?;
    module.define_singleton_method(
        "_dtype_str_repr",
        function!(crate::functions::misc::dtype_str_repr, 1),
    )?;
    module.define_singleton_method(
        "_as_struct",
        function!(crate::functions::lazy::as_struct, 1),
    )?;
    module.define_singleton_method(
        "_arg_where",
        function!(crate::functions::lazy::arg_where, 1),
    )?;
    module.define_singleton_method(
        "_get_idx_type",
        function!(crate::functions::meta::get_idx_type, 0),
    )?;
    module.define_singleton_method(
        "_threadpool_size",
        function!(crate::functions::meta::threadpool_size, 0),
    )?;
    module.define_singleton_method(
        "_enable_string_cache",
        function!(crate::functions::string_cache::enable_string_cache, 0),
    )?;
    module.define_singleton_method(
        "_disable_string_cache",
        function!(crate::functions::string_cache::disable_string_cache, 0),
    )?;
    module.define_singleton_method(
        "_using_string_cache",
        function!(crate::functions::string_cache::using_string_cache, 0),
    )?;
    module.define_singleton_method(
        "_set_float_fmt",
        function!(crate::functions::meta::set_float_fmt, 1),
    )?;
    module.define_singleton_method(
        "_get_float_fmt",
        function!(crate::functions::meta::get_float_fmt, 0),
    )?;
    module.define_singleton_method(
        "_set_random_seed",
        function!(crate::functions::random::set_random_seed, 1),
    )?;

    let class = module.define_class("RbBatchedCsv", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbBatchedCsv::new, -1))?;
    class.define_method("next_batches", method!(RbBatchedCsv::next_batches, 1))?;

    let class = module.define_class("RbDataFrame", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbDataFrame::init, 1))?;
    class.define_singleton_method("read_csv", function!(RbDataFrame::read_csv, -1))?;
    class.define_singleton_method("read_parquet", function!(RbDataFrame::read_parquet, 9))?;
    class.define_singleton_method("read_ipc", function!(RbDataFrame::read_ipc, 6))?;
    class.define_singleton_method("read_avro", function!(RbDataFrame::read_avro, 4))?;
    class.define_singleton_method("read_rows", function!(RbDataFrame::read_rows, 3))?;
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
    class.define_method("to_numo", method!(RbDataFrame::to_numo, 0))?;
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
    class.define_method("pivot_expr", method!(RbDataFrame::pivot_expr, 7))?;
    class.define_method("partition_by", method!(RbDataFrame::partition_by, 3))?;
    class.define_method("shift", method!(RbDataFrame::shift, 1))?;
    class.define_method("lazy", method!(RbDataFrame::lazy, 0))?;
    class.define_method("max", method!(RbDataFrame::max, 0))?;
    class.define_method("min", method!(RbDataFrame::min, 0))?;
    class.define_method("sum", method!(RbDataFrame::sum, 0))?;
    class.define_method("mean", method!(RbDataFrame::mean, 0))?;
    class.define_method("std", method!(RbDataFrame::std, 1))?;
    class.define_method("var", method!(RbDataFrame::var, 1))?;
    class.define_method("median", method!(RbDataFrame::median, 0))?;
    class.define_method("mean_horizontal", method!(RbDataFrame::mean_horizontal, 1))?;
    class.define_method("max_horizontal", method!(RbDataFrame::max_horizontal, 0))?;
    class.define_method("min_horizontal", method!(RbDataFrame::min_horizontal, 0))?;
    class.define_method("sum_horizontal", method!(RbDataFrame::sum_horizontal, 1))?;
    class.define_method("quantile", method!(RbDataFrame::quantile, 2))?;
    class.define_method("to_dummies", method!(RbDataFrame::to_dummies, 3))?;
    class.define_method("null_count", method!(RbDataFrame::null_count, 0))?;
    class.define_method("apply", method!(RbDataFrame::apply, 3))?;
    class.define_method("shrink_to_fit", method!(RbDataFrame::shrink_to_fit, 0))?;
    class.define_method("hash_rows", method!(RbDataFrame::hash_rows, 4))?;
    class.define_method("transpose", method!(RbDataFrame::transpose, 2))?;
    class.define_method("upsample", method!(RbDataFrame::upsample, 5))?;
    class.define_method("to_struct", method!(RbDataFrame::to_struct, 1))?;
    class.define_method("unnest", method!(RbDataFrame::unnest, 1))?;

    let class = module.define_class("RbExpr", ruby.class_object())?;
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
    class.define_method("implode", method!(RbExpr::implode, 0))?;
    class.define_method("quantile", method!(RbExpr::quantile, 2))?;
    class.define_method("cut", method!(RbExpr::cut, 4))?;
    class.define_method("qcut", method!(RbExpr::qcut, 5))?;
    class.define_method("qcut_uniform", method!(RbExpr::qcut_uniform, 5))?;
    class.define_method("rle", method!(RbExpr::rle, 0))?;
    class.define_method("rle_id", method!(RbExpr::rle_id, 0))?;
    class.define_method("agg_groups", method!(RbExpr::agg_groups, 0))?;
    class.define_method("count", method!(RbExpr::count, 0))?;
    class.define_method("value_counts", method!(RbExpr::value_counts, 2))?;
    class.define_method("unique_counts", method!(RbExpr::unique_counts, 0))?;
    class.define_method("null_count", method!(RbExpr::null_count, 0))?;
    class.define_method("cast", method!(RbExpr::cast, 2))?;
    class.define_method("sort_with", method!(RbExpr::sort_with, 2))?;
    class.define_method("arg_sort", method!(RbExpr::arg_sort, 2))?;
    class.define_method("top_k", method!(RbExpr::top_k, 1))?;
    class.define_method("bottom_k", method!(RbExpr::bottom_k, 1))?;
    class.define_method("peak_min", method!(RbExpr::peak_min, 0))?;
    class.define_method("peak_max", method!(RbExpr::peak_max, 0))?;
    class.define_method("arg_max", method!(RbExpr::arg_max, 0))?;
    class.define_method("arg_min", method!(RbExpr::arg_min, 0))?;
    class.define_method("search_sorted", method!(RbExpr::search_sorted, 2))?;
    class.define_method("gather", method!(RbExpr::gather, 1))?;
    class.define_method("sort_by", method!(RbExpr::sort_by, 2))?;
    class.define_method("backward_fill", method!(RbExpr::backward_fill, 1))?;
    class.define_method("forward_fill", method!(RbExpr::forward_fill, 1))?;
    class.define_method("shift", method!(RbExpr::shift, 2))?;
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
    class.define_method("approx_n_unique", method!(RbExpr::approx_n_unique, 0))?;
    class.define_method("is_first_distinct", method!(RbExpr::is_first_distinct, 0))?;
    class.define_method("is_last_distinct", method!(RbExpr::is_last_distinct, 0))?;
    class.define_method("explode", method!(RbExpr::explode, 0))?;
    class.define_method("gather_every", method!(RbExpr::gather_every, 1))?;
    class.define_method("tail", method!(RbExpr::tail, 1))?;
    class.define_method("head", method!(RbExpr::head, 1))?;
    class.define_method("slice", method!(RbExpr::slice, 2))?;
    class.define_method("append", method!(RbExpr::append, 2))?;
    class.define_method("rechunk", method!(RbExpr::rechunk, 0))?;
    class.define_method("round", method!(RbExpr::round, 1))?;
    class.define_method("floor", method!(RbExpr::floor, 0))?;
    class.define_method("ceil", method!(RbExpr::ceil, 0))?;
    class.define_method("clip", method!(RbExpr::clip, 2))?;
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
    class.define_method("cum_sum", method!(RbExpr::cum_sum, 1))?;
    class.define_method("cum_max", method!(RbExpr::cum_max, 1))?;
    class.define_method("cum_min", method!(RbExpr::cum_min, 1))?;
    class.define_method("cum_prod", method!(RbExpr::cum_prod, 1))?;
    class.define_method("product", method!(RbExpr::product, 0))?;
    class.define_method("shrink_dtype", method!(RbExpr::shrink_dtype, 0))?;
    class.define_method("str_to_date", method!(RbExpr::str_to_date, 4))?;
    class.define_method("str_to_datetime", method!(RbExpr::str_to_datetime, 7))?;
    class.define_method("str_to_time", method!(RbExpr::str_to_time, 3))?;
    class.define_method("str_strip_chars", method!(RbExpr::str_strip_chars, 1))?;
    class.define_method(
        "str_strip_chars_start",
        method!(RbExpr::str_strip_chars_start, 1),
    )?;
    class.define_method(
        "str_strip_chars_end",
        method!(RbExpr::str_strip_chars_end, 1),
    )?;
    class.define_method("str_strip_prefix", method!(RbExpr::str_strip_prefix, 1))?;
    class.define_method("str_strip_suffix", method!(RbExpr::str_strip_suffix, 1))?;
    class.define_method("str_slice", method!(RbExpr::str_slice, 2))?;
    class.define_method("str_explode", method!(RbExpr::str_explode, 0))?;
    class.define_method("str_to_uppercase", method!(RbExpr::str_to_uppercase, 0))?;
    class.define_method("str_to_lowercase", method!(RbExpr::str_to_lowercase, 0))?;
    class.define_method("str_len_bytes", method!(RbExpr::str_len_bytes, 0))?;
    class.define_method("str_len_chars", method!(RbExpr::str_len_chars, 0))?;
    class.define_method("str_replace_n", method!(RbExpr::str_replace_n, 4))?;
    class.define_method("str_replace_all", method!(RbExpr::str_replace_all, 3))?;
    class.define_method("str_zfill", method!(RbExpr::str_zfill, 1))?;
    class.define_method("str_pad_start", method!(RbExpr::str_pad_start, 2))?;
    class.define_method("str_pad_end", method!(RbExpr::str_pad_end, 2))?;
    class.define_method("str_contains", method!(RbExpr::str_contains, 3))?;
    class.define_method("str_ends_with", method!(RbExpr::str_ends_with, 1))?;
    class.define_method("str_starts_with", method!(RbExpr::str_starts_with, 1))?;
    class.define_method("array_max", method!(RbExpr::array_max, 0))?;
    class.define_method("array_min", method!(RbExpr::array_min, 0))?;
    class.define_method("array_sum", method!(RbExpr::array_sum, 0))?;
    class.define_method("binary_contains", method!(RbExpr::bin_contains, 1))?;
    class.define_method("binary_ends_with", method!(RbExpr::bin_ends_with, 1))?;
    class.define_method("binary_starts_with", method!(RbExpr::bin_starts_with, 1))?;
    class.define_method("str_hex_encode", method!(RbExpr::str_hex_encode, 0))?;
    class.define_method("str_hex_decode", method!(RbExpr::str_hex_decode, 1))?;
    class.define_method("str_base64_encode", method!(RbExpr::str_base64_encode, 0))?;
    class.define_method("str_base64_decode", method!(RbExpr::str_base64_decode, 1))?;
    class.define_method("str_to_integer", method!(RbExpr::str_to_integer, 2))?;
    class.define_method("str_json_extract", method!(RbExpr::str_json_extract, 2))?;
    class.define_method("binary_hex_encode", method!(RbExpr::bin_hex_encode, 0))?;
    class.define_method("binary_hex_decode", method!(RbExpr::bin_hex_decode, 1))?;
    class.define_method(
        "binary_base64_encode",
        method!(RbExpr::bin_base64_encode, 0),
    )?;
    class.define_method(
        "binary_base64_decode",
        method!(RbExpr::bin_base64_decode, 1),
    )?;
    class.define_method(
        "str_json_path_match",
        method!(RbExpr::str_json_path_match, 1),
    )?;
    class.define_method("str_extract", method!(RbExpr::str_extract, 2))?;
    class.define_method("str_extract_all", method!(RbExpr::str_extract_all, 1))?;
    class.define_method("str_count_matches", method!(RbExpr::str_count_matches, 2))?;
    class.define_method("strftime", method!(RbExpr::dt_to_string, 1))?;
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
    class.define_method("list_len", method!(RbExpr::list_len, 0))?;
    class.define_method("list_contains", method!(RbExpr::list_contains, 1))?;
    class.define_method("list_count_matches", method!(RbExpr::list_count_matches, 1))?;
    class.define_method("year", method!(RbExpr::dt_year, 0))?;
    class.define_method("dt_is_leap_year", method!(RbExpr::dt_is_leap_year, 0))?;
    class.define_method("iso_year", method!(RbExpr::dt_iso_year, 0))?;
    class.define_method("quarter", method!(RbExpr::dt_quarter, 0))?;
    class.define_method("month", method!(RbExpr::dt_month, 0))?;
    class.define_method("week", method!(RbExpr::dt_week, 0))?;
    class.define_method("weekday", method!(RbExpr::dt_weekday, 0))?;
    class.define_method("day", method!(RbExpr::dt_day, 0))?;
    class.define_method("ordinal_day", method!(RbExpr::dt_ordinal_day, 0))?;
    class.define_method("dt_time", method!(RbExpr::dt_time, 0))?;
    class.define_method("dt_date", method!(RbExpr::dt_date, 0))?;
    class.define_method("dt_datetime", method!(RbExpr::dt_datetime, 0))?;
    class.define_method("hour", method!(RbExpr::dt_hour, 0))?;
    class.define_method("minute", method!(RbExpr::dt_minute, 0))?;
    class.define_method("second", method!(RbExpr::dt_second, 0))?;
    class.define_method("millisecond", method!(RbExpr::dt_millisecond, 0))?;
    class.define_method("microsecond", method!(RbExpr::dt_microsecond, 0))?;
    class.define_method("nanosecond", method!(RbExpr::dt_nanosecond, 0))?;
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
    class.define_method("timestamp", method!(RbExpr::dt_timestamp, 1))?;
    class.define_method("dt_offset_by", method!(RbExpr::dt_offset_by, 1))?;
    class.define_method("dt_epoch_seconds", method!(RbExpr::dt_epoch_seconds, 0))?;
    class.define_method("dt_with_time_unit", method!(RbExpr::dt_with_time_unit, 1))?;
    class.define_method(
        "dt_convert_time_zone",
        method!(RbExpr::dt_convert_time_zone, 1),
    )?;
    class.define_method("dt_cast_time_unit", method!(RbExpr::dt_cast_time_unit, 1))?;
    class.define_method(
        "dt_replace_time_zone",
        method!(RbExpr::dt_replace_time_zone, 2),
    )?;
    class.define_method("dt_truncate", method!(RbExpr::dt_truncate, 2))?;
    class.define_method("dt_month_start", method!(RbExpr::dt_month_start, 0))?;
    class.define_method("dt_month_end", method!(RbExpr::dt_month_end, 0))?;
    class.define_method("dt_round", method!(RbExpr::dt_round, 2))?;
    class.define_method("dt_combine", method!(RbExpr::dt_combine, 2))?;
    class.define_method("map", method!(RbExpr::map, 3))?;
    class.define_method("dot", method!(RbExpr::dot, 1))?;
    class.define_method("reinterpret", method!(RbExpr::reinterpret, 1))?;
    class.define_method("mode", method!(RbExpr::mode, 0))?;
    class.define_method("exclude", method!(RbExpr::exclude, 1))?;
    class.define_method("interpolate", method!(RbExpr::interpolate, 1))?;
    class.define_method("rolling_sum", method!(RbExpr::rolling_sum, 6))?;
    class.define_method("rolling_min", method!(RbExpr::rolling_min, 6))?;
    class.define_method("rolling_max", method!(RbExpr::rolling_max, 6))?;
    class.define_method("rolling_mean", method!(RbExpr::rolling_mean, 6))?;
    class.define_method("rolling_std", method!(RbExpr::rolling_std, 7))?;
    class.define_method("rolling_var", method!(RbExpr::rolling_var, 7))?;
    class.define_method("rolling_median", method!(RbExpr::rolling_median, 6))?;
    class.define_method("rolling_quantile", method!(RbExpr::rolling_quantile, 8))?;
    class.define_method("rolling_skew", method!(RbExpr::rolling_skew, 2))?;
    class.define_method("lower_bound", method!(RbExpr::lower_bound, 0))?;
    class.define_method("upper_bound", method!(RbExpr::upper_bound, 0))?;
    class.define_method("list_max", method!(RbExpr::list_max, 0))?;
    class.define_method("list_min", method!(RbExpr::list_min, 0))?;
    class.define_method("list_sum", method!(RbExpr::list_sum, 0))?;
    class.define_method("list_take", method!(RbExpr::list_take, 2))?;
    class.define_method("list_mean", method!(RbExpr::list_mean, 0))?;
    class.define_method("list_sort", method!(RbExpr::list_sort, 1))?;
    class.define_method("list_reverse", method!(RbExpr::list_reverse, 0))?;
    class.define_method("list_unique", method!(RbExpr::list_unique, 1))?;
    class.define_method("list_get", method!(RbExpr::list_get, 1))?;
    class.define_method("list_join", method!(RbExpr::list_join, 1))?;
    class.define_method("list_arg_min", method!(RbExpr::list_arg_min, 0))?;
    class.define_method("list_arg_max", method!(RbExpr::list_arg_max, 0))?;
    class.define_method("list_diff", method!(RbExpr::list_diff, 2))?;
    class.define_method("list_shift", method!(RbExpr::list_shift, 1))?;
    class.define_method("list_slice", method!(RbExpr::list_slice, 2))?;
    class.define_method("list_eval", method!(RbExpr::list_eval, 2))?;
    class.define_method("cumulative_eval", method!(RbExpr::cumulative_eval, 3))?;
    class.define_method("list_to_struct", method!(RbExpr::list_to_struct, 3))?;
    class.define_method("rank", method!(RbExpr::rank, 3))?;
    class.define_method("diff", method!(RbExpr::diff, 2))?;
    class.define_method("pct_change", method!(RbExpr::pct_change, 1))?;
    class.define_method("skew", method!(RbExpr::skew, 1))?;
    class.define_method("kurtosis", method!(RbExpr::kurtosis, 2))?;
    class.define_method("str_concat", method!(RbExpr::str_concat, 2))?;
    class.define_method("cat_set_ordering", method!(RbExpr::cat_set_ordering, 1))?;
    class.define_method("reshape", method!(RbExpr::reshape, 1))?;
    class.define_method("cum_count", method!(RbExpr::cum_count, 1))?;
    class.define_method("to_physical", method!(RbExpr::to_physical, 0))?;
    class.define_method("shuffle", method!(RbExpr::shuffle, 1))?;
    class.define_method("sample_n", method!(RbExpr::sample_n, 4))?;
    class.define_method("sample_frac", method!(RbExpr::sample_frac, 4))?;
    class.define_method("ewm_mean", method!(RbExpr::ewm_mean, 4))?;
    class.define_method("ewm_std", method!(RbExpr::ewm_std, 5))?;
    class.define_method("ewm_var", method!(RbExpr::ewm_var, 5))?;
    class.define_method("extend_constant", method!(RbExpr::extend_constant, 2))?;
    class.define_method("any", method!(RbExpr::any, 1))?;
    class.define_method("all", method!(RbExpr::all, 1))?;
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
    class.define_method("set_sorted_flag", method!(RbExpr::set_sorted_flag, 1))?;

    // meta
    class.define_method("meta_pop", method!(RbExpr::meta_pop, 0))?;
    class.define_method("meta_eq", method!(RbExpr::meta_eq, 1))?;
    class.define_method("meta_roots", method!(RbExpr::meta_root_names, 0))?;
    class.define_method("meta_output_name", method!(RbExpr::meta_output_name, 0))?;
    class.define_method("meta_undo_aliases", method!(RbExpr::meta_undo_aliases, 0))?;
    class.define_method(
        "meta_has_multiple_outputs",
        method!(RbExpr::meta_has_multiple_outputs, 0),
    )?;
    class.define_method(
        "meta_is_regex_projection",
        method!(RbExpr::meta_is_regex_projection, 0),
    )?;

    // name
    class.define_method("name_keep", method!(RbExpr::name_keep, 0))?;
    class.define_method("name_map", method!(RbExpr::name_map, 1))?;
    class.define_method("name_prefix", method!(RbExpr::name_prefix, 1))?;
    class.define_method("name_suffix", method!(RbExpr::name_suffix, 1))?;
    class.define_method("name_to_lowercase", method!(RbExpr::name_to_lowercase, 0))?;
    class.define_method("name_to_uppercase", method!(RbExpr::name_to_uppercase, 0))?;

    // maybe add to different class
    class.define_singleton_method("col", function!(crate::functions::lazy::col, 1))?;
    class.define_singleton_method("count", function!(crate::functions::lazy::count, 0))?;
    class.define_singleton_method("first", function!(crate::functions::lazy::first, 0))?;
    class.define_singleton_method("last", function!(crate::functions::lazy::last, 0))?;
    class.define_singleton_method("cols", function!(crate::functions::lazy::cols, 1))?;
    class.define_singleton_method("fold", function!(crate::functions::lazy::fold, 3))?;
    class.define_singleton_method("cumfold", function!(crate::functions::lazy::cumfold, 4))?;
    class.define_singleton_method("lit", function!(crate::functions::lazy::lit, 2))?;
    class.define_singleton_method(
        "int_range",
        function!(crate::functions::range::int_range, 4),
    )?;
    class.define_singleton_method(
        "int_ranges",
        function!(crate::functions::range::int_ranges, 4),
    )?;
    class.define_singleton_method("repeat", function!(crate::functions::lazy::repeat, 3))?;
    class.define_singleton_method(
        "pearson_corr",
        function!(crate::functions::lazy::pearson_corr, 3),
    )?;
    class.define_singleton_method(
        "spearman_rank_corr",
        function!(crate::functions::lazy::spearman_rank_corr, 4),
    )?;
    class.define_singleton_method("cov", function!(crate::functions::lazy::cov, 3))?;
    class.define_singleton_method(
        "arg_sort_by",
        function!(crate::functions::lazy::arg_sort_by, 2),
    )?;
    class.define_singleton_method("when", function!(crate::functions::whenthen::when, 1))?;
    class.define_singleton_method(
        "concat_str",
        function!(crate::functions::lazy::concat_str, 2),
    )?;
    class.define_singleton_method(
        "concat_lst",
        function!(crate::functions::lazy::concat_lst, 1),
    )?;

    let class = module.define_class("RbLazyFrame", ruby.class_object())?;
    class.define_singleton_method("read_json", function!(RbLazyFrame::read_json, 1))?;
    class.define_singleton_method(
        "new_from_ndjson",
        function!(RbLazyFrame::new_from_ndjson, 7),
    )?;
    class.define_singleton_method("new_from_csv", function!(RbLazyFrame::new_from_csv, -1))?;
    class.define_singleton_method(
        "new_from_parquet",
        function!(RbLazyFrame::new_from_parquet, 10),
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
        method!(RbLazyFrame::optimization_toggle, 8),
    )?;
    class.define_method("sort", method!(RbLazyFrame::sort, 4))?;
    class.define_method("sort_by_exprs", method!(RbLazyFrame::sort_by_exprs, 4))?;
    class.define_method("cache", method!(RbLazyFrame::cache, 0))?;
    class.define_method("collect", method!(RbLazyFrame::collect, 0))?;
    class.define_method("sink_parquet", method!(RbLazyFrame::sink_parquet, 7))?;
    class.define_method("fetch", method!(RbLazyFrame::fetch, 1))?;
    class.define_method("filter", method!(RbLazyFrame::filter, 1))?;
    class.define_method("select", method!(RbLazyFrame::select, 1))?;
    class.define_method("group_by", method!(RbLazyFrame::group_by, 2))?;
    class.define_method(
        "group_by_rolling",
        method!(RbLazyFrame::group_by_rolling, 6),
    )?;
    class.define_method(
        "group_by_dynamic",
        method!(RbLazyFrame::group_by_dynamic, 10),
    )?;
    class.define_method("with_context", method!(RbLazyFrame::with_context, 1))?;
    class.define_method("join_asof", method!(RbLazyFrame::join_asof, 11))?;
    class.define_method("join", method!(RbLazyFrame::join, 7))?;
    class.define_method("with_columns", method!(RbLazyFrame::with_columns, 1))?;
    class.define_method("rename", method!(RbLazyFrame::rename, 2))?;
    class.define_method("reverse", method!(RbLazyFrame::reverse, 0))?;
    class.define_method("shift", method!(RbLazyFrame::shift, 2))?;
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
    class.define_method("melt", method!(RbLazyFrame::melt, 5))?;
    class.define_method("with_row_count", method!(RbLazyFrame::with_row_count, 2))?;
    class.define_method("drop_columns", method!(RbLazyFrame::drop_columns, 1))?;
    class.define_method("_clone", method!(RbLazyFrame::clone, 0))?;
    class.define_method("columns", method!(RbLazyFrame::columns, 0))?;
    class.define_method("dtypes", method!(RbLazyFrame::dtypes, 0))?;
    class.define_method("schema", method!(RbLazyFrame::schema, 0))?;
    class.define_method("unnest", method!(RbLazyFrame::unnest, 1))?;
    class.define_method("width", method!(RbLazyFrame::width, 0))?;

    let class = module.define_class("RbLazyGroupBy", ruby.class_object())?;
    class.define_method("agg", method!(RbLazyGroupBy::agg, 1))?;
    class.define_method("head", method!(RbLazyGroupBy::head, 1))?;
    class.define_method("tail", method!(RbLazyGroupBy::tail, 1))?;

    let class = module.define_class("RbSeries", ruby.class_object())?;
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
    class.define_singleton_method(
        "new_from_anyvalues",
        function!(RbSeries::new_from_anyvalues, 3),
    )?;
    class.define_singleton_method("new_str", function!(RbSeries::new_str, 3))?;
    class.define_singleton_method("new_binary", function!(RbSeries::new_binary, 3))?;
    class.define_singleton_method("new_null", function!(RbSeries::new_null, 3))?;
    class.define_singleton_method("new_object", function!(RbSeries::new_object, 3))?;
    class.define_singleton_method("new_series_list", function!(RbSeries::new_series_list, 3))?;
    class.define_singleton_method("new_array", function!(RbSeries::new_array, 5))?;
    class.define_singleton_method("new_decimal", function!(RbSeries::new_decimal, 3))?;
    class.define_singleton_method("repeat", function!(RbSeries::repeat, 4))?;
    class.define_method("struct_unnest", method!(RbSeries::struct_unnest, 0))?;
    class.define_method(
        "is_sorted_flag",
        method!(RbSeries::is_sorted_ascending_flag, 0),
    )?;
    class.define_method(
        "is_sorted_reverse_flag",
        method!(RbSeries::is_sorted_descending_flag, 0),
    )?;
    class.define_method(
        "can_fast_explode_flag",
        method!(RbSeries::can_fast_explode_flag, 0),
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
    class.define_method("set_sorted", method!(RbSeries::set_sorted_flag, 1))?;
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
    class.define_method("to_dummies", method!(RbSeries::to_dummies, 2))?;
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

    // str comp
    class.define_method("eq_str", method!(RbSeries::eq_str, 1))?;
    class.define_method("neq_str", method!(RbSeries::neq_str, 1))?;
    class.define_method("gt_str", method!(RbSeries::gt_str, 1))?;
    class.define_method("gt_eq_str", method!(RbSeries::gt_eq_str, 1))?;
    class.define_method("lt_str", method!(RbSeries::lt_str, 1))?;
    class.define_method("lt_eq_str", method!(RbSeries::lt_eq_str, 1))?;

    // npy
    class.define_method("to_numo", method!(RbSeries::to_numo, 0))?;

    // extra
    class.define_method("extend_constant", method!(RbSeries::extend_constant, 2))?;

    let class = module.define_class("RbWhen", ruby.class_object())?;
    class.define_method("_then", method!(RbWhen::then, 1))?;

    let class = module.define_class("RbWhenThen", ruby.class_object())?;
    class.define_method("otherwise", method!(RbThen::overwise, 1))?;

    // sql
    let class = module.define_class("RbSQLContext", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbSQLContext::new, 0))?;
    class.define_method("execute", method!(RbSQLContext::execute, 1))?;
    class.define_method("get_tables", method!(RbSQLContext::get_tables, 0))?;
    class.define_method("register", method!(RbSQLContext::register, 2))?;
    class.define_method("unregister", method!(RbSQLContext::unregister, 1))?;

    Ok(())
}
