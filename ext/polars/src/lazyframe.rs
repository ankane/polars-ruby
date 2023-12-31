use magnus::{IntoValue, RArray, RHash, TryConvert, Value};
use polars::io::RowCount;
use polars::io::cloud::CloudOptions;
use polars::lazy::frame::LazyFrame;
use polars::prelude::*;
use std::cell::RefCell;
use std::io::{BufWriter, Read};
use std::path::PathBuf;

use crate::conversion::*;
use crate::expr::rb_exprs_to_exprs;
use crate::file::get_file_like;
use crate::{RbDataFrame, RbExpr, RbLazyGroupBy, RbPolarsErr, RbResult, RbValueError};

#[magnus::wrap(class = "Polars::RbLazyFrame")]
#[derive(Clone)]
pub struct RbLazyFrame {
    pub ldf: LazyFrame,
}

impl RbLazyFrame {
    fn get_schema(&self) -> RbResult<SchemaRef> {
        let schema = self.ldf.schema().map_err(RbPolarsErr::from)?;
        Ok(schema)
    }
}

impl From<LazyFrame> for RbLazyFrame {
    fn from(ldf: LazyFrame) -> Self {
        RbLazyFrame { ldf }
    }
}

impl RbLazyFrame {
    pub fn read_json(rb_f: Value) -> RbResult<Self> {
        // it is faster to first read to memory and then parse: https://github.com/serde-rs/json/issues/160
        // so don't bother with files.
        let mut json = String::new();
        let _ = get_file_like(rb_f, false)?
            .read_to_string(&mut json)
            .unwrap();

        // Safety
        // we skipped the serializing/deserializing of the static in lifetime in `DataType`
        // so we actually don't have a lifetime at all when serializing.

        // &str still has a lifetime. Bit its ok, because we drop it immediately
        // in this scope
        let json = unsafe { std::mem::transmute::<&'_ str, &'static str>(json.as_str()) };

        let lp = serde_json::from_str::<LogicalPlan>(json)
            .map_err(|err| RbValueError::new_err(format!("{:?}", err)))?;
        Ok(LazyFrame::from(lp).into())
    }

    pub fn new_from_ndjson(
        path: String,
        infer_schema_length: Option<usize>,
        batch_size: Option<usize>,
        n_rows: Option<usize>,
        low_memory: bool,
        rechunk: bool,
        row_count: Option<(String, IdxSize)>,
    ) -> RbResult<Self> {
        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });

        let lf = LazyJsonLineReader::new(path)
            .with_infer_schema_length(infer_schema_length)
            .with_batch_size(batch_size)
            .with_n_rows(n_rows)
            .low_memory(low_memory)
            .with_rechunk(rechunk)
            .with_row_count(row_count)
            .finish()
            .map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    pub fn new_from_csv(arguments: &[Value]) -> RbResult<Self> {
        // start arguments
        // this pattern is needed for more than 16
        let path = String::try_convert(arguments[0])?;
        let separator = String::try_convert(arguments[1])?;
        let has_header = bool::try_convert(arguments[2])?;
        let ignore_errors = bool::try_convert(arguments[3])?;
        let skip_rows = usize::try_convert(arguments[4])?;
        let n_rows = Option::<usize>::try_convert(arguments[5])?;
        let cache = bool::try_convert(arguments[6])?;
        let overwrite_dtype = Option::<Vec<(String, Wrap<DataType>)>>::try_convert(arguments[7])?;
        let low_memory = bool::try_convert(arguments[8])?;
        let comment_char = Option::<String>::try_convert(arguments[9])?;
        let quote_char = Option::<String>::try_convert(arguments[10])?;
        let null_values = Option::<Wrap<NullValues>>::try_convert(arguments[11])?;
        let infer_schema_length = Option::<usize>::try_convert(arguments[12])?;
        let with_schema_modify = Option::<Value>::try_convert(arguments[13])?;
        let rechunk = bool::try_convert(arguments[14])?;
        let skip_rows_after_header = usize::try_convert(arguments[15])?;
        let encoding = Wrap::<CsvEncoding>::try_convert(arguments[16])?;
        let row_count = Option::<(String, IdxSize)>::try_convert(arguments[17])?;
        let try_parse_dates = bool::try_convert(arguments[18])?;
        let eol_char = String::try_convert(arguments[19])?;
        // end arguments

        let null_values = null_values.map(|w| w.0);
        let comment_char = comment_char.map(|s| s.as_bytes()[0]);
        let quote_char = quote_char.map(|s| s.as_bytes()[0]);
        let separator = separator.as_bytes()[0];
        let eol_char = eol_char.as_bytes()[0];

        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });

        let overwrite_dtype = overwrite_dtype.map(|overwrite_dtype| {
            overwrite_dtype
                .into_iter()
                .map(|(name, dtype)| Field::new(&name, dtype.0))
                .collect::<Schema>()
        });
        let r = LazyCsvReader::new(path)
            .with_infer_schema_length(infer_schema_length)
            .with_separator(separator)
            .has_header(has_header)
            .with_ignore_errors(ignore_errors)
            .with_skip_rows(skip_rows)
            .with_n_rows(n_rows)
            .with_cache(cache)
            .with_dtype_overwrite(overwrite_dtype.as_ref())
            .low_memory(low_memory)
            .with_comment_char(comment_char)
            .with_quote_char(quote_char)
            .with_end_of_line_char(eol_char)
            .with_rechunk(rechunk)
            .with_skip_rows_after_header(skip_rows_after_header)
            .with_encoding(encoding.0)
            .with_row_count(row_count)
            .with_try_parse_dates(try_parse_dates)
            .with_null_values(null_values);

        if let Some(_lambda) = with_schema_modify {
            todo!();
        }

        Ok(r.finish().map_err(RbPolarsErr::from)?.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_from_parquet(
        path: String,
        n_rows: Option<usize>,
        cache: bool,
        parallel: Wrap<ParallelStrategy>,
        rechunk: bool,
        row_count: Option<(String, IdxSize)>,
        low_memory: bool,
        storage_options: Option<Vec<(String,String)>>,
        use_statistics: bool,
        hive_partitioning: bool,
    ) -> RbResult<Self> {
        let cloud_options = storage_options
            .map(|kv| Self::parse_cloud_options(&path, kv))
            .transpose()?;

        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });
        let args = ScanArgsParquet {
            n_rows,
            cache,
            parallel: parallel.0,
            rechunk,
            row_count,
            low_memory,
            cloud_options: cloud_options,
            use_statistics,
            hive_partitioning,
        };
        let lf = LazyFrame::scan_parquet(path, args).map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    pub(crate) fn parse_cloud_options(uri: &str, kv: Vec<(String, String)>) -> RbResult<CloudOptions> {
        let out = CloudOptions::from_untyped_config(uri, kv).map_err(RbPolarsErr::from)?;
        Ok(out)
    }

    pub fn new_from_ipc(
        path: String,
        n_rows: Option<usize>,
        cache: bool,
        rechunk: bool,
        row_count: Option<(String, IdxSize)>,
        memory_map: bool,
    ) -> RbResult<Self> {
        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });
        let args = ScanArgsIpc {
            n_rows,
            cache,
            rechunk,
            row_count,
            memmap: memory_map,
        };
        let lf = LazyFrame::scan_ipc(path, args).map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    pub fn write_json(&self, rb_f: Value) -> RbResult<()> {
        let file = BufWriter::new(get_file_like(rb_f, true)?);
        serde_json::to_writer(file, &self.ldf.logical_plan)
            .map_err(|err| RbValueError::new_err(format!("{:?}", err)))?;
        Ok(())
    }

    pub fn describe_plan(&self) -> String {
        self.ldf.describe_plan()
    }

    pub fn describe_optimized_plan(&self) -> RbResult<String> {
        let result = self
            .ldf
            .describe_optimized_plan()
            .map_err(RbPolarsErr::from)?;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn optimization_toggle(
        &self,
        type_coercion: bool,
        predicate_pushdown: bool,
        projection_pushdown: bool,
        simplify_expr: bool,
        slice_pushdown: bool,
        cse: bool,
        allow_streaming: bool,
        _eager: bool,
    ) -> RbLazyFrame {
        let ldf = self.ldf.clone();
        let ldf = ldf
            .with_type_coercion(type_coercion)
            .with_predicate_pushdown(predicate_pushdown)
            .with_simplify_expr(simplify_expr)
            .with_slice_pushdown(slice_pushdown)
            .with_comm_subplan_elim(cse)
            .with_streaming(allow_streaming)
            ._with_eager(_eager)
            .with_projection_pushdown(projection_pushdown);
        ldf.into()
    }

    pub fn sort(
        &self,
        by_column: String,
        reverse: bool,
        nulls_last: bool,
        maintain_order: bool,
    ) -> Self {
        let ldf = self.ldf.clone();
        ldf.sort(
            &by_column,
            SortOptions {
                descending: reverse,
                nulls_last,
                multithreaded: true,
                maintain_order,
            },
        )
        .into()
    }

    pub fn sort_by_exprs(
        &self,
        by_column: RArray,
        reverse: Vec<bool>,
        nulls_last: bool,
        maintain_order: bool,
    ) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        let exprs = rb_exprs_to_exprs(by_column)?;
        Ok(ldf
            .sort_by_exprs(exprs, reverse, nulls_last, maintain_order)
            .into())
    }

    pub fn cache(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.cache().into()
    }

    pub fn collect(&self) -> RbResult<RbDataFrame> {
        let ldf = self.ldf.clone();
        let df = ldf.collect().map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sink_parquet(
        &self,
        path: PathBuf,
        compression: String,
        compression_level: Option<i32>,
        statistics: bool,
        row_group_size: Option<usize>,
        data_pagesize_limit: Option<usize>,
        maintain_order: bool,
    ) -> RbResult<()> {
        let compression = parse_parquet_compression(&compression, compression_level)?;

        let options = ParquetWriteOptions {
            compression,
            statistics,
            row_group_size,
            data_pagesize_limit,
            maintain_order,
        };

        let ldf = self.ldf.clone();
        ldf.sink_parquet(path, options).map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn fetch(&self, n_rows: usize) -> RbResult<RbDataFrame> {
        let ldf = self.ldf.clone();
        let df = ldf.fetch(n_rows).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn filter(&self, predicate: &RbExpr) -> Self {
        let ldf = self.ldf.clone();
        ldf.filter(predicate.inner.clone()).into()
    }

    pub fn select(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        let exprs = rb_exprs_to_exprs(exprs)?;
        Ok(ldf.select(exprs).into())
    }

    pub fn group_by(&self, by: RArray, maintain_order: bool) -> RbResult<RbLazyGroupBy> {
        let ldf = self.ldf.clone();
        let by = rb_exprs_to_exprs(by)?;
        let lazy_gb = if maintain_order {
            ldf.group_by_stable(by)
        } else {
            ldf.group_by(by)
        };
        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    pub fn group_by_rolling(
        &self,
        index_column: &RbExpr,
        period: String,
        offset: String,
        closed: Wrap<ClosedWindow>,
        by: RArray,
        check_sorted: bool,
    ) -> RbResult<RbLazyGroupBy> {
        let closed_window = closed.0;
        let ldf = self.ldf.clone();
        let by = rb_exprs_to_exprs(by)?;
        let lazy_gb = ldf.group_by_rolling(
            index_column.inner.clone(),
            by,
            RollingGroupOptions {
                index_column: "".into(),
                period: Duration::parse(&period),
                offset: Duration::parse(&offset),
                closed_window,
                check_sorted,
            },
        );

        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn group_by_dynamic(
        &self,
        index_column: &RbExpr,
        every: String,
        period: String,
        offset: String,
        label: Wrap<Label>,
        include_boundaries: bool,
        closed: Wrap<ClosedWindow>,
        by: RArray,
        start_by: Wrap<StartBy>,
        check_sorted: bool,
    ) -> RbResult<RbLazyGroupBy> {
        let closed_window = closed.0;
        let by = rb_exprs_to_exprs(by)?;
        let ldf = self.ldf.clone();
        let lazy_gb = ldf.group_by_dynamic(
            index_column.inner.clone(),
            by,
            DynamicGroupOptions {
                every: Duration::parse(&every),
                period: Duration::parse(&period),
                offset: Duration::parse(&offset),
                label: label.0,
                include_boundaries,
                closed_window,
                start_by: start_by.0,
                check_sorted,
                ..Default::default()
            },
        );

        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    pub fn with_context(&self, contexts: RArray) -> RbResult<Self> {
        let contexts = contexts
            .each()
            .map(|v| TryConvert::try_convert(v.unwrap()))
            .collect::<RbResult<Vec<&RbLazyFrame>>>()?;
        let contexts = contexts
            .into_iter()
            .map(|ldf| ldf.ldf.clone())
            .collect::<Vec<_>>();
        Ok(self.ldf.clone().with_context(contexts).into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn join_asof(
        &self,
        other: &RbLazyFrame,
        left_on: &RbExpr,
        right_on: &RbExpr,
        left_by: Option<Vec<String>>,
        right_by: Option<Vec<String>>,
        allow_parallel: bool,
        force_parallel: bool,
        suffix: String,
        strategy: Wrap<AsofStrategy>,
        tolerance: Option<Wrap<AnyValue<'_>>>,
        tolerance_str: Option<String>,
    ) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        let other = other.ldf.clone();
        let left_on = left_on.inner.clone();
        let right_on = right_on.inner.clone();
        Ok(ldf
            .join_builder()
            .with(other)
            .left_on([left_on])
            .right_on([right_on])
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .how(JoinType::AsOf(AsOfOptions {
                strategy: strategy.0,
                left_by: left_by.map(strings_to_smartstrings),
                right_by: right_by.map(strings_to_smartstrings),
                tolerance: tolerance.map(|t| t.0.into_static().unwrap()),
                tolerance_str: tolerance_str.map(|s| s.into()),
            }))
            .suffix(suffix)
            .finish()
            .into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn join(
        &self,
        other: &RbLazyFrame,
        left_on: RArray,
        right_on: RArray,
        allow_parallel: bool,
        force_parallel: bool,
        how: Wrap<JoinType>,
        suffix: String,
    ) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        let other = other.ldf.clone();
        let left_on = rb_exprs_to_exprs(left_on)?;
        let right_on = rb_exprs_to_exprs(right_on)?;

        Ok(ldf
            .join_builder()
            .with(other)
            .left_on(left_on)
            .right_on(right_on)
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .how(how.0)
            .suffix(suffix)
            .finish()
            .into())
    }

    pub fn with_columns(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        Ok(ldf.with_columns(rb_exprs_to_exprs(exprs)?).into())
    }

    pub fn rename(&self, existing: Vec<String>, new: Vec<String>) -> Self {
        let ldf = self.ldf.clone();
        ldf.rename(existing, new).into()
    }

    pub fn reverse(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.reverse().into()
    }

    pub fn shift(&self, n: &RbExpr, fill_value: Option<&RbExpr>) -> Self {
        let lf = self.ldf.clone();
        let out = match fill_value {
            Some(v) => lf.shift_and_fill(n.inner.clone(), v.inner.clone()),
            None => lf.shift(n.inner.clone()),
        };
        out.into()
    }

    pub fn fill_nan(&self, fill_value: &RbExpr) -> Self {
        let ldf = self.ldf.clone();
        ldf.fill_nan(fill_value.inner.clone()).into()
    }

    pub fn min(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.min().into()
    }

    pub fn max(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.max().into()
    }

    pub fn sum(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.sum().into()
    }

    pub fn mean(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.mean().into()
    }

    pub fn std(&self, ddof: u8) -> Self {
        let ldf = self.ldf.clone();
        ldf.std(ddof).into()
    }

    pub fn var(&self, ddof: u8) -> Self {
        let ldf = self.ldf.clone();
        ldf.var(ddof).into()
    }

    pub fn median(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.median().into()
    }

    pub fn quantile(
        &self,
        quantile: &RbExpr,
        interpolation: Wrap<QuantileInterpolOptions>,
    ) -> Self {
        let ldf = self.ldf.clone();
        ldf.quantile(quantile.inner.clone(), interpolation.0).into()
    }

    pub fn explode(&self, column: RArray) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        let column = rb_exprs_to_exprs(column)?;
        Ok(ldf.explode(column).into())
    }

    pub fn unique(
        &self,
        maintain_order: bool,
        subset: Option<Vec<String>>,
        keep: Wrap<UniqueKeepStrategy>,
    ) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        Ok(match maintain_order {
            true => ldf.unique_stable(subset, keep.0),
            false => ldf.unique(subset, keep.0),
        }
        .into())
    }

    pub fn drop_nulls(&self, subset: Option<Vec<String>>) -> Self {
        let ldf = self.ldf.clone();
        ldf.drop_nulls(subset.map(|v| v.into_iter().map(|s| col(&s)).collect()))
            .into()
    }

    pub fn slice(&self, offset: i64, len: Option<IdxSize>) -> Self {
        let ldf = self.ldf.clone();
        ldf.slice(offset, len.unwrap_or(IdxSize::MAX)).into()
    }

    pub fn tail(&self, n: IdxSize) -> Self {
        let ldf = self.ldf.clone();
        ldf.tail(n).into()
    }

    pub fn melt(
        &self,
        id_vars: Vec<String>,
        value_vars: Vec<String>,
        value_name: Option<String>,
        variable_name: Option<String>,
        streamable: bool,
    ) -> Self {
        let args = MeltArgs {
            id_vars: strings_to_smartstrings(id_vars),
            value_vars: strings_to_smartstrings(value_vars),
            value_name: value_name.map(|s| s.into()),
            variable_name: variable_name.map(|s| s.into()),
            streamable,
        };

        let ldf = self.ldf.clone();
        ldf.melt(args).into()
    }

    pub fn with_row_count(&self, name: String, offset: Option<IdxSize>) -> Self {
        let ldf = self.ldf.clone();
        ldf.with_row_count(&name, offset).into()
    }

    pub fn drop_columns(&self, cols: Vec<String>) -> Self {
        let ldf = self.ldf.clone();
        ldf.drop_columns(cols).into()
    }

    pub fn clone(&self) -> Self {
        self.ldf.clone().into()
    }

    pub fn columns(&self) -> RbResult<RArray> {
        let schema = self.get_schema()?;
        let iter = schema.iter_names().map(|s| s.as_str());
        Ok(RArray::from_iter(iter))
    }

    pub fn dtypes(&self) -> RbResult<RArray> {
        let schema = self.get_schema()?;
        let iter = schema.iter_dtypes().map(|dt| Wrap(dt.clone()).into_value());
        Ok(RArray::from_iter(iter))
    }

    pub fn schema(&self) -> RbResult<RHash> {
        let schema = self.get_schema()?;
        let schema_dict = RHash::new();

        schema.iter_fields().for_each(|fld| {
            // TODO remove unwrap
            schema_dict
                .aset::<String, Value>(
                    fld.name().to_string(),
                    Wrap(fld.data_type().clone()).into_value(),
                )
                .unwrap();
        });
        Ok(schema_dict)
    }

    pub fn unnest(&self, cols: Vec<String>) -> Self {
        self.ldf.clone().unnest(cols).into()
    }

    pub fn width(&self) -> RbResult<usize> {
        Ok(self.get_schema()?.len())
    }
}
