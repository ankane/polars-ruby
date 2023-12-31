module Polars
  # Representation of a Lazy computation graph/query against a DataFrame.
  class LazyFrame
    # @private
    attr_accessor :_ldf

    # Create a new LazyFrame.
    def initialize(data = nil, schema: nil, schema_overrides: nil, orient: nil, infer_schema_length: 100, nan_to_null: false)
      self._ldf = (
        DataFrame.new(
          data,
          schema: schema,
          schema_overrides: schema_overrides,
          orient: orient,
          infer_schema_length: infer_schema_length,
          nan_to_null: nan_to_null
        )
        .lazy
        ._ldf
      )
    end

    # @private
    def self._from_rbldf(rb_ldf)
      ldf = LazyFrame.allocate
      ldf._ldf = rb_ldf
      ldf
    end

    # @private
    def self._scan_csv(
      file,
      has_header: true,
      sep: ",",
      comment_char: nil,
      quote_char: '"',
      skip_rows: 0,
      dtypes: nil,
      null_values: nil,
      ignore_errors: false,
      cache: true,
      with_column_names: nil,
      infer_schema_length: 100,
      n_rows: nil,
      encoding: "utf8",
      low_memory: false,
      rechunk: true,
      skip_rows_after_header: 0,
      row_count_name: nil,
      row_count_offset: 0,
      parse_dates: false,
      eol_char: "\n"
    )
      dtype_list = nil
      if !dtypes.nil?
        dtype_list = []
        dtypes.each do |k, v|
          dtype_list << [k, Utils.rb_type_to_dtype(v)]
        end
      end
      processed_null_values = Utils._process_null_values(null_values)

      _from_rbldf(
        RbLazyFrame.new_from_csv(
          file,
          sep,
          has_header,
          ignore_errors,
          skip_rows,
          n_rows,
          cache,
          dtype_list,
          low_memory,
          comment_char,
          quote_char,
          processed_null_values,
          infer_schema_length,
          with_column_names,
          rechunk,
          skip_rows_after_header,
          encoding,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          parse_dates,
          eol_char
        )
      )
    end

    # @private
    def self._scan_parquet(
      file,
      n_rows: nil,
      cache: true,
      parallel: "auto",
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0,
      storage_options: {},
      low_memory: false,
      use_statistics: true,
      hive_partitioning: true
    )

      storage_opts = storage_options
        .transform_keys(&:to_s)
        .transform_values(&:to_s)
        .to_a

      _from_rbldf(
        RbLazyFrame.new_from_parquet(
          file,
          n_rows,
          cache,
          parallel,
          rechunk,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          low_memory,
          storage_opts,
          use_statistics,
          hive_partitioning
        )
      )
    end

    # @private
    def self._scan_ipc(
      file,
      n_rows: nil,
      cache: true,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0,
      storage_options: nil,
      memory_map: true
    )
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      _from_rbldf(
        RbLazyFrame.new_from_ipc(
          file,
          n_rows,
          cache,
          rechunk,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          memory_map
        )
      )
    end

    # @private
    def self._scan_ndjson(
      file,
      infer_schema_length: nil,
      batch_size: nil,
      n_rows: nil,
      low_memory: false,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0
    )
      _from_rbldf(
        RbLazyFrame.new_from_ndjson(
          file,
          infer_schema_length,
          batch_size,
          n_rows,
          low_memory,
          rechunk,
          Utils._prepare_row_count_args(row_count_name, row_count_offset)
        )
      )
    end

    # def self.from_json
    # end

    # Read a logical plan from a JSON file to construct a LazyFrame.
    #
    # @param file [String]
    #   Path to a file or a file-like object.
    #
    # @return [LazyFrame]
    def self.read_json(file)
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      Utils.wrap_ldf(RbLazyFrame.read_json(file))
    end

    # Get or set column names.
    #
    # @return [Array]
    #
    # @example
    #   df = (
    #      Polars::DataFrame.new(
    #        {
    #          "foo" => [1, 2, 3],
    #          "bar" => [6, 7, 8],
    #          "ham" => ["a", "b", "c"]
    #        }
    #      )
    #      .lazy
    #      .select(["foo", "bar"])
    #   )
    #   df.columns
    #   # => ["foo", "bar"]
    def columns
      _ldf.columns
    end

    # Get dtypes of columns in LazyFrame.
    #
    # @return [Array]
    #
    # @example
    #   lf = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   lf.dtypes
    #   # => [Polars::Int64, Polars::Float64, Polars::Utf8]
    def dtypes
      _ldf.dtypes
    end

    # Get the schema.
    #
    # @return [Hash]
    #
    # @example
    #   lf = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   lf.schema
    #   # => {"foo"=>Polars::Int64, "bar"=>Polars::Float64, "ham"=>Polars::Utf8}
    def schema
      _ldf.schema
    end

    # Get the width of the LazyFrame.
    #
    # @return [Integer]
    #
    # @example
    #   lf = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]}).lazy
    #   lf.width
    #   # => 2
    def width
      _ldf.width
    end

    # Check if LazyFrame includes key.
    #
    # @return [Boolean]
    def include?(key)
      columns.include?(key)
    end

    # clone handled by initialize_copy

    # def [](item)
    # end

    # Returns a string representing the LazyFrame.
    #
    # @return [String]
    def to_s
      <<~EOS
        naive plan: (run LazyFrame#describe_optimized_plan to see the optimized plan)

        #{describe_plan}
      EOS
    end

    # Write the logical plan of this LazyFrame to a file or string in JSON format.
    #
    # @param file [String]
    #   File path to which the result should be written.
    #
    # @return [nil]
    def write_json(file)
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end
      _ldf.write_json(file)
      nil
    end

    # Offers a structured way to apply a sequence of user-defined functions (UDFs).
    #
    # @param func [Object]
    #   Callable; will receive the frame as the first parameter,
    #   followed by any given args/kwargs.
    # @param args [Object]
    #   Arguments to pass to the UDF.
    # @param kwargs [Object]
    #   Keyword arguments to pass to the UDF.
    #
    # @return [LazyFrame]
    #
    # @example
    #   cast_str_to_int = lambda do |data, col_name:|
    #     data.with_column(Polars.col(col_name).cast(:i64))
    #   end
    #
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => ["10", "20", "30", "40"]}).lazy
    #   df.pipe(cast_str_to_int, col_name: "b").collect()
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 10  │
    #   # │ 2   ┆ 20  │
    #   # │ 3   ┆ 30  │
    #   # │ 4   ┆ 40  │
    #   # └─────┴─────┘
    def pipe(func, *args, **kwargs, &block)
      func.call(self, *args, **kwargs, &block)
    end

    # Create a string representation of the unoptimized query plan.
    #
    # @return [String]
    def describe_plan
      _ldf.describe_plan
    end

    # Create a string representation of the optimized query plan.
    #
    # @return [String]
    def describe_optimized_plan(
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      slice_pushdown: true,
      common_subplan_elimination: true,
      allow_streaming: false
    )
      ldf = _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        common_subplan_elimination,
        allow_streaming,
        false
      )

      ldf.describe_optimized_plan
    end

    # def show_graph
    # end

    # Sort the DataFrame.
    #
    # Sorting can be done by:
    #
    # - A single column name
    # - An expression
    # - Multiple expressions
    #
    # @param by [Object]
    #   Column (expressions) to sort by.
    # @param reverse [Boolean]
    #   Sort in descending order.
    # @param nulls_last [Boolean]
    #   Place null values last. Can only be used if sorted by a single column.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   df.sort("foo", reverse: true).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # └─────┴─────┴─────┘
    def sort(by, reverse: false, nulls_last: false, maintain_order: false)
      if by.is_a?(String)
        return _from_rbldf(_ldf.sort(by, reverse, nulls_last, maintain_order))
      end
      if Utils.bool?(reverse)
        reverse = [reverse]
      end

      by = Utils.selection_to_rbexpr_list(by)
      _from_rbldf(_ldf.sort_by_exprs(by, reverse, nulls_last, maintain_order))
    end

    # def profile
    # end

    # Collect into a DataFrame.
    #
    # Note: use {#fetch} if you want to run your query on the first `n` rows
    # only. This can be a huge time saver in debugging queries.
    #
    # @param type_coercion [Boolean]
    #   Do type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Do predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Do projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param string_cache [Boolean]
    #   This argument is deprecated. Please set the string cache globally.
    #   The argument will be ignored
    # @param no_optimization [Boolean]
    #   Turn off (certain) optimizations.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization.
    # @param common_subplan_elimination [Boolean]
    #   Will try to cache branching subplans that occur on self-joins or unions.
    # @param allow_streaming [Boolean]
    #   Run parts of the query in a streaming fashion (this is in an alpha state)
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [1, 2, 3, 4, 5, 6],
    #       "c" => [6, 5, 4, 3, 2, 1]
    #     }
    #   ).lazy
    #   df.group_by("a", maintain_order: true).agg(Polars.all.sum).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ a   ┆ 4   ┆ 10  │
    #   # │ b   ┆ 11  ┆ 10  │
    #   # │ c   ┆ 6   ┆ 1   │
    #   # └─────┴─────┴─────┘
    def collect(
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      string_cache: false,
      no_optimization: false,
      slice_pushdown: true,
      common_subplan_elimination: true,
      allow_streaming: false,
      _eager: false
    )
      if no_optimization
        predicate_pushdown = false
        projection_pushdown = false
        slice_pushdown = false
        common_subplan_elimination = false
      end

      if allow_streaming
        common_subplan_elimination = false
      end

      ldf = _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        common_subplan_elimination,
        allow_streaming,
        _eager
      )
      Utils.wrap_df(ldf.collect)
    end

    # Persists a LazyFrame at the provided path.
    #
    # This allows streaming results that are larger than RAM to be written to disk.
    #
    # @param path [String]
    #   File path to which the file should be written.
    # @param compression ["lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"]
    #   Choose "zstd" for good compression performance.
    #   Choose "lz4" for fast compression/decompression.
    #   Choose "snappy" for more backwards compatibility guarantees
    #   when you deal with older parquet readers.
    # @param compression_level [Integer]
    #   The level of compression to use. Higher compression means smaller files on
    #   disk.
    #
    #   - "gzip" : min-level: 0, max-level: 10.
    #   - "brotli" : min-level: 0, max-level: 11.
    #   - "zstd" : min-level: 1, max-level: 22.
    # @param statistics [Boolean]
    #   Write statistics to the parquet headers. This requires extra compute.
    # @param row_group_size [Integer]
    #   Size of the row groups in number of rows.
    #   If `nil` (default), the chunks of the `DataFrame` are
    #   used. Writing in smaller chunks may reduce memory pressure and improve
    #   writing speeds.
    # @param data_pagesize_limit [Integer]
    #   Size limit of individual data pages.
    #   If not set defaults to 1024 * 1024 bytes
    # @param maintain_order [Boolean]
    #   Maintain the order in which data is processed.
    #   Setting this to `false` will  be slightly faster.
    # @param type_coercion [Boolean]
    #   Do type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Do predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Do projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param no_optimization [Boolean]
    #   Turn off (certain) optimizations.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization.
    #
    # @return [DataFrame]
    #
    # @example
    #   lf = Polars.scan_csv("/path/to/my_larger_than_ram_file.csv")
    #   lf.sink_parquet("out.parquet")
    def sink_parquet(
      path,
      compression: "zstd",
      compression_level: nil,
      statistics: false,
      row_group_size: nil,
      data_pagesize_limit: nil,
      maintain_order: true,
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      no_optimization: false,
      slice_pushdown: true
    )
      if no_optimization
        predicate_pushdown = false
        projection_pushdown = false
        slice_pushdown = false
      end

      lf = _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        false,
        true,
        false
      )
      lf.sink_parquet(
        path,
        compression,
        compression_level,
        statistics,
        row_group_size,
        data_pagesize_limit,
        maintain_order
      )
    end

    # Collect a small number of rows for debugging purposes.
    #
    # Fetch is like a {#collect} operation, but it overwrites the number of rows
    # read by every scan operation. This is a utility that helps debug a query on a
    # smaller number of rows.
    #
    # Note that the fetch does not guarantee the final number of rows in the
    # DataFrame. Filter, join operations and a lower number of rows available in the
    # scanned file influence the final number of rows.
    #
    # @param n_rows [Integer]
    #   Collect n_rows from the data sources.
    # @param type_coercion [Boolean]
    #   Run type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Run predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Run projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param string_cache [Boolean]
    #   This argument is deprecated. Please set the string cache globally.
    #   The argument will be ignored
    # @param no_optimization [Boolean]
    #   Turn off optimizations.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization
    # @param common_subplan_elimination [Boolean]
    #   Will try to cache branching subplans that occur on self-joins or unions.
    # @param allow_streaming [Boolean]
    #   Run parts of the query in a streaming fashion (this is in an alpha state)
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [1, 2, 3, 4, 5, 6],
    #       "c" => [6, 5, 4, 3, 2, 1]
    #     }
    #   ).lazy
    #   df.group_by("a", maintain_order: true).agg(Polars.all.sum).fetch(2)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ a   ┆ 1   ┆ 6   │
    #   # │ b   ┆ 2   ┆ 5   │
    #   # └─────┴─────┴─────┘
    def fetch(
      n_rows = 500,
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      string_cache: false,
      no_optimization: false,
      slice_pushdown: true,
      common_subplan_elimination: true,
      allow_streaming: false
    )
      if no_optimization
        predicate_pushdown = false
        projection_pushdown = false
        slice_pushdown = false
        common_subplan_elimination = false
      end

      ldf = _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        common_subplan_elimination,
        allow_streaming,
        false
      )
      Utils.wrap_df(ldf.fetch(n_rows))
    end

    # Return lazy representation, i.e. itself.
    #
    # Useful for writing code that expects either a `DataFrame` or
    # `LazyFrame`.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [nil, 2, 3, 4],
    #       "b" => [0.5, nil, 2.5, 13],
    #       "c" => [true, true, false, nil]
    #     }
    #   )
    #   df.lazy
    def lazy
      self
    end

    # Cache the result once the execution of the physical plan hits this node.
    #
    # @return [LazyFrame]
    def cache
      _from_rbldf(_ldf.cache)
    end

    # Create an empty copy of the current LazyFrame.
    #
    # The copy has an identical schema but no data.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [nil, 2, 3, 4],
    #       "b" => [0.5, nil, 2.5, 13],
    #       "c" => [true, true, false, nil],
    #     }
    #   ).lazy
    #   df.cleared.fetch
    #   # =>
    #   # shape: (0, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ a   ┆ b   ┆ c    │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ f64 ┆ bool │
    #   # ╞═════╪═════╪══════╡
    #   # └─────┴─────┴──────┘
    def cleared
      DataFrame.new(columns: schema).lazy
    end

    # Filter the rows in the DataFrame based on a predicate expression.
    #
    # @param predicate [Object]
    #   Expression that evaluates to a boolean Series.
    #
    # @return [LazyFrame]
    #
    # @example Filter on one condition:
    #   lf = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   lf.filter(Polars.col("foo") < 3).collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # └─────┴─────┴─────┘
    #
    # @example Filter on multiple conditions:
    #   lf.filter((Polars.col("foo") < 3) & (Polars.col("ham") == "a")).collect
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # └─────┴─────┴─────┘
    def filter(predicate)
      _from_rbldf(
        _ldf.filter(
          Utils.expr_to_lit_or_expr(predicate, str_to_lit: false)._rbexpr
        )
      )
    end

    # Select columns from this DataFrame.
    #
    # @param exprs [Object]
    #   Column or columns to select.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"],
    #     }
    #   ).lazy
    #   df.select("foo").collect
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 3   │
    #   # └─────┘
    #
    # @example
    #   df.select(["foo", "bar"]).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 6   │
    #   # │ 2   ┆ 7   │
    #   # │ 3   ┆ 8   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.select(Polars.col("foo") + 1).collect
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # │ 3   │
    #   # │ 4   │
    #   # └─────┘
    #
    # @example
    #   df.select([Polars.col("foo") + 1, Polars.col("bar") + 1]).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 2   ┆ 7   │
    #   # │ 3   ┆ 8   │
    #   # │ 4   ┆ 9   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.select(Polars.when(Polars.col("foo") > 2).then(10).otherwise(0)).collect
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────┐
    #   # │ literal │
    #   # │ ---     │
    #   # │ i64     │
    #   # ╞═════════╡
    #   # │ 0       │
    #   # │ 0       │
    #   # │ 10      │
    #   # └─────────┘
    def select(exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      _from_rbldf(_ldf.select(exprs))
    end

    # Start a group by operation.
    #
    # @param by [Object]
    #   Column(s) to group by.
    # @param maintain_order [Boolean]
    #   Make sure that the order of the groups remain consistent. This is more
    #   expensive than a default group by.
    #
    # @return [LazyGroupBy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [1, 2, 3, 4, 5, 6],
    #       "c" => [6, 5, 4, 3, 2, 1]
    #     }
    #   ).lazy
    #   df.group_by("a", maintain_order: true).agg(Polars.col("b").sum).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 4   │
    #   # │ b   ┆ 11  │
    #   # │ c   ┆ 6   │
    #   # └─────┴─────┘
    def group_by(by, maintain_order: false)
      rbexprs_by = Utils.selection_to_rbexpr_list(by)
      lgb = _ldf.group_by(rbexprs_by, maintain_order)
      LazyGroupBy.new(lgb)
    end
    alias_method :groupby, :group_by
    alias_method :group, :group_by

    # Create rolling groups based on a time column.
    #
    # Also works for index values of type `:i32` or `:i64`.
    #
    # Different from a `dynamic_group_by` the windows are now determined by the
    # individual values and are not of constant intervals. For constant intervals
    # use *group_by_dynamic*.
    #
    # The `period` and `offset` arguments are created either from a timedelta, or
    # by using the following string language:
    #
    # - 1ns   (1 nanosecond)
    # - 1us   (1 microsecond)
    # - 1ms   (1 millisecond)
    # - 1s    (1 second)
    # - 1m    (1 minute)
    # - 1h    (1 hour)
    # - 1d    (1 day)
    # - 1w    (1 week)
    # - 1mo   (1 calendar month)
    # - 1y    (1 calendar year)
    # - 1i    (1 index count)
    #
    # Or combine them:
    # "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # In case of a group_by_rolling on an integer column, the windows are defined by:
    #
    # - "1i"      # length 1
    # - "10i"     # length 10
    #
    # @param index_column [Object]
    #   Column used to group based on the time window.
    #   Often to type Date/Datetime
    #   This column must be sorted in ascending order. If not the output will not
    #   make sense.
    #
    #   In case of a rolling group by on indices, dtype needs to be one of
    #   `:i32`, `:i64`. Note that `:i32` gets temporarily cast to `:i64`, so if
    #   performance matters use an `:i64` column.
    # @param period [Object]
    #   Length of the window.
    # @param offset [Object]
    #   Offset of the window. Default is -period.
    # @param closed ["right", "left", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    # @param by [Object]
    #   Also group by this column/these columns.
    # @param check_sorted [Boolean]
    #   When the `by` argument is given, polars can not check sortedness
    #   by the metadata and has to do a full scan on the index column to
    #   verify data is sorted. This is expensive. If you are sure the
    #   data within the by groups is sorted, you can set this to `false`.
    #   Doing so incorrectly will lead to incorrect output
    #
    # @return [LazyFrame]
    #
    # @example
    #   dates = [
    #     "2020-01-01 13:45:48",
    #     "2020-01-01 16:42:13",
    #     "2020-01-01 16:45:09",
    #     "2020-01-02 18:12:48",
    #     "2020-01-03 19:45:32",
    #     "2020-01-08 23:16:43"
    #   ]
    #   df = Polars::LazyFrame.new({"dt" => dates, "a" => [3, 7, 5, 9, 2, 1]}).with_column(
    #     Polars.col("dt").str.strptime(Polars::Datetime).set_sorted
    #   )
    #   df.group_by_rolling(index_column: "dt", period: "2d").agg(
    #     [
    #       Polars.sum("a").alias("sum_a"),
    #       Polars.min("a").alias("min_a"),
    #       Polars.max("a").alias("max_a")
    #     ]
    #   ).collect
    #   # =>
    #   # shape: (6, 4)
    #   # ┌─────────────────────┬───────┬───────┬───────┐
    #   # │ dt                  ┆ sum_a ┆ min_a ┆ max_a │
    #   # │ ---                 ┆ ---   ┆ ---   ┆ ---   │
    #   # │ datetime[μs]        ┆ i64   ┆ i64   ┆ i64   │
    #   # ╞═════════════════════╪═══════╪═══════╪═══════╡
    #   # │ 2020-01-01 13:45:48 ┆ 3     ┆ 3     ┆ 3     │
    #   # │ 2020-01-01 16:42:13 ┆ 10    ┆ 3     ┆ 7     │
    #   # │ 2020-01-01 16:45:09 ┆ 15    ┆ 3     ┆ 7     │
    #   # │ 2020-01-02 18:12:48 ┆ 24    ┆ 3     ┆ 9     │
    #   # │ 2020-01-03 19:45:32 ┆ 11    ┆ 2     ┆ 9     │
    #   # │ 2020-01-08 23:16:43 ┆ 1     ┆ 1     ┆ 1     │
    #   # └─────────────────────┴───────┴───────┴───────┘
    def group_by_rolling(
      index_column:,
      period:,
      offset: nil,
      closed: "right",
      by: nil,
      check_sorted: true
    )
      index_column = Utils.parse_as_expression(index_column)
      if offset.nil?
        offset = "-#{period}"
      end

      rbexprs_by = by.nil? ? [] : Utils.selection_to_rbexpr_list(by)
      period = Utils._timedelta_to_pl_duration(period)
      offset = Utils._timedelta_to_pl_duration(offset)

      lgb = _ldf.group_by_rolling(
        index_column, period, offset, closed, rbexprs_by, check_sorted
      )
      LazyGroupBy.new(lgb)
    end
    alias_method :groupby_rolling, :group_by_rolling

    # Group based on a time value (or index value of type `:i32`, `:i64`).
    #
    # Time windows are calculated and rows are assigned to windows. Different from a
    # normal group by is that a row can be member of multiple groups. The time/index
    # window could be seen as a rolling window, with a window size determined by
    # dates/times/values instead of slots in the DataFrame.
    #
    # A window is defined by:
    #
    # - every: interval of the window
    # - period: length of the window
    # - offset: offset of the window
    #
    # The `every`, `period` and `offset` arguments are created with
    # the following string language:
    #
    # - 1ns   (1 nanosecond)
    # - 1us   (1 microsecond)
    # - 1ms   (1 millisecond)
    # - 1s    (1 second)
    # - 1m    (1 minute)
    # - 1h    (1 hour)
    # - 1d    (1 day)
    # - 1w    (1 week)
    # - 1mo   (1 calendar month)
    # - 1y    (1 calendar year)
    # - 1i    (1 index count)
    #
    # Or combine them:
    # "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # In case of a group_by_dynamic on an integer column, the windows are defined by:
    #
    # - "1i"      # length 1
    # - "10i"     # length 10
    #
    # @param index_column [Object]
    #   Column used to group based on the time window.
    #   Often to type Date/Datetime
    #   This column must be sorted in ascending order. If not the output will not
    #   make sense.
    #
    #   In case of a dynamic group by on indices, dtype needs to be one of
    #   `:i32`, `:i64`. Note that `:i32` gets temporarily cast to `:i64`, so if
    #   performance matters use an `:i64` column.
    # @param every [Object]
    #   Interval of the window.
    # @param period [Object]
    #   Length of the window, if None it is equal to 'every'.
    # @param offset [Object]
    #   Offset of the window if None and period is None it will be equal to negative
    #   `every`.
    # @param truncate [Boolean]
    #   Truncate the time value to the window lower bound.
    # @param include_boundaries [Boolean]
    #   Add the lower and upper bound of the window to the "_lower_bound" and
    #   "_upper_bound" columns. This will impact performance because it's harder to
    #   parallelize
    # @param closed ["right", "left", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    # @param by [Object]
    #   Also group by this column/these columns
    # @param check_sorted [Boolean]
    #   When the `by` argument is given, polars can not check sortedness
    #   by the metadata and has to do a full scan on the index column to
    #   verify data is sorted. This is expensive. If you are sure the
    #   data within the by groups is sorted, you can set this to `false`.
    #   Doing so incorrectly will lead to incorrect output.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "time" => Polars.date_range(
    #         DateTime.new(2021, 12, 16),
    #         DateTime.new(2021, 12, 16, 3),
    #         "30m"
    #       ),
    #       "n" => 0..6
    #     }
    #   )
    #   # =>
    #   # shape: (7, 2)
    #   # ┌─────────────────────┬─────┐
    #   # │ time                ┆ n   │
    #   # │ ---                 ┆ --- │
    #   # │ datetime[μs]        ┆ i64 │
    #   # ╞═════════════════════╪═════╡
    #   # │ 2021-12-16 00:00:00 ┆ 0   │
    #   # │ 2021-12-16 00:30:00 ┆ 1   │
    #   # │ 2021-12-16 01:00:00 ┆ 2   │
    #   # │ 2021-12-16 01:30:00 ┆ 3   │
    #   # │ 2021-12-16 02:00:00 ┆ 4   │
    #   # │ 2021-12-16 02:30:00 ┆ 5   │
    #   # │ 2021-12-16 03:00:00 ┆ 6   │
    #   # └─────────────────────┴─────┘
    #
    # @example Group by windows of 1 hour starting at 2021-12-16 00:00:00.
    #   df.group_by_dynamic("time", every: "1h", closed: "right").agg(
    #     [
    #       Polars.col("time").min.alias("time_min"),
    #       Polars.col("time").max.alias("time_max")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┐
    #   # │ time                ┆ time_min            ┆ time_max            │
    #   # │ ---                 ┆ ---                 ┆ ---                 │
    #   # │ datetime[μs]        ┆ datetime[μs]        ┆ datetime[μs]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╡
    #   # │ 2021-12-15 23:00:00 ┆ 2021-12-16 00:00:00 ┆ 2021-12-16 00:00:00 │
    #   # │ 2021-12-16 00:00:00 ┆ 2021-12-16 00:30:00 ┆ 2021-12-16 01:00:00 │
    #   # │ 2021-12-16 01:00:00 ┆ 2021-12-16 01:30:00 ┆ 2021-12-16 02:00:00 │
    #   # │ 2021-12-16 02:00:00 ┆ 2021-12-16 02:30:00 ┆ 2021-12-16 03:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┘
    #
    # @example The window boundaries can also be added to the aggregation result.
    #   df.group_by_dynamic(
    #     "time", every: "1h", include_boundaries: true, closed: "right"
    #   ).agg([Polars.col("time").count.alias("time_count")])
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┬────────────┐
    #   # │ _lower_boundary     ┆ _upper_boundary     ┆ time                ┆ time_count │
    #   # │ ---                 ┆ ---                 ┆ ---                 ┆ ---        │
    #   # │ datetime[μs]        ┆ datetime[μs]        ┆ datetime[μs]        ┆ u32        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╪════════════╡
    #   # │ 2021-12-15 23:00:00 ┆ 2021-12-16 00:00:00 ┆ 2021-12-15 23:00:00 ┆ 1          │
    #   # │ 2021-12-16 00:00:00 ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 00:00:00 ┆ 2          │
    #   # │ 2021-12-16 01:00:00 ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 01:00:00 ┆ 2          │
    #   # │ 2021-12-16 02:00:00 ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 02:00:00 ┆ 2          │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┴────────────┘
    #
    # @example When closed="left", should not include right end of interval.
    #   df.group_by_dynamic("time", every: "1h", closed: "left").agg(
    #     [
    #       Polars.col("time").count.alias("time_count"),
    #       Polars.col("time").alias("time_agg_list")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────────────┬────────────┬───────────────────────────────────┐
    #   # │ time                ┆ time_count ┆ time_agg_list                     │
    #   # │ ---                 ┆ ---        ┆ ---                               │
    #   # │ datetime[μs]        ┆ u32        ┆ list[datetime[μs]]                │
    #   # ╞═════════════════════╪════════════╪═══════════════════════════════════╡
    #   # │ 2021-12-16 00:00:00 ┆ 2          ┆ [2021-12-16 00:00:00, 2021-12-16… │
    #   # │ 2021-12-16 01:00:00 ┆ 2          ┆ [2021-12-16 01:00:00, 2021-12-16… │
    #   # │ 2021-12-16 02:00:00 ┆ 2          ┆ [2021-12-16 02:00:00, 2021-12-16… │
    #   # │ 2021-12-16 03:00:00 ┆ 1          ┆ [2021-12-16 03:00:00]             │
    #   # └─────────────────────┴────────────┴───────────────────────────────────┘
    #
    # @example When closed="both" the time values at the window boundaries belong to 2 groups.
    #   df.group_by_dynamic("time", every: "1h", closed: "both").agg(
    #     [Polars.col("time").count.alias("time_count")]
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────────────────┬────────────┐
    #   # │ time                ┆ time_count │
    #   # │ ---                 ┆ ---        │
    #   # │ datetime[μs]        ┆ u32        │
    #   # ╞═════════════════════╪════════════╡
    #   # │ 2021-12-15 23:00:00 ┆ 1          │
    #   # │ 2021-12-16 00:00:00 ┆ 3          │
    #   # │ 2021-12-16 01:00:00 ┆ 3          │
    #   # │ 2021-12-16 02:00:00 ┆ 3          │
    #   # │ 2021-12-16 03:00:00 ┆ 1          │
    #   # └─────────────────────┴────────────┘
    #
    # @example Dynamic group bys can also be combined with grouping on normal keys.
    #   df = Polars::DataFrame.new(
    #     {
    #       "time" => Polars.date_range(
    #         DateTime.new(2021, 12, 16),
    #         DateTime.new(2021, 12, 16, 3),
    #         "30m"
    #       ),
    #       "groups" => ["a", "a", "a", "b", "b", "a", "a"]
    #     }
    #   )
    #   df.group_by_dynamic(
    #     "time",
    #     every: "1h",
    #     closed: "both",
    #     by: "groups",
    #     include_boundaries: true
    #   ).agg([Polars.col("time").count.alias("time_count")])
    #   # =>
    #   # shape: (7, 5)
    #   # ┌────────┬─────────────────────┬─────────────────────┬─────────────────────┬────────────┐
    #   # │ groups ┆ _lower_boundary     ┆ _upper_boundary     ┆ time                ┆ time_count │
    #   # │ ---    ┆ ---                 ┆ ---                 ┆ ---                 ┆ ---        │
    #   # │ str    ┆ datetime[μs]        ┆ datetime[μs]        ┆ datetime[μs]        ┆ u32        │
    #   # ╞════════╪═════════════════════╪═════════════════════╪═════════════════════╪════════════╡
    #   # │ a      ┆ 2021-12-15 23:00:00 ┆ 2021-12-16 00:00:00 ┆ 2021-12-15 23:00:00 ┆ 1          │
    #   # │ a      ┆ 2021-12-16 00:00:00 ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 00:00:00 ┆ 3          │
    #   # │ a      ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 01:00:00 ┆ 1          │
    #   # │ a      ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 02:00:00 ┆ 2          │
    #   # │ a      ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 04:00:00 ┆ 2021-12-16 03:00:00 ┆ 1          │
    #   # │ b      ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 01:00:00 ┆ 2          │
    #   # │ b      ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 02:00:00 ┆ 1          │
    #   # └────────┴─────────────────────┴─────────────────────┴─────────────────────┴────────────┘
    #
    # @example Dynamic group by on an index column.
    #   df = Polars::DataFrame.new(
    #     {
    #       "idx" => Polars.arange(0, 6, eager: true),
    #       "A" => ["A", "A", "B", "B", "B", "C"]
    #     }
    #   )
    #   df.group_by_dynamic(
    #     "idx",
    #     every: "2i",
    #     period: "3i",
    #     include_boundaries: true,
    #     closed: "right"
    #   ).agg(Polars.col("A").alias("A_agg_list"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────────────────┬─────────────────┬─────┬─────────────────┐
    #   # │ _lower_boundary ┆ _upper_boundary ┆ idx ┆ A_agg_list      │
    #   # │ ---             ┆ ---             ┆ --- ┆ ---             │
    #   # │ i64             ┆ i64             ┆ i64 ┆ list[str]       │
    #   # ╞═════════════════╪═════════════════╪═════╪═════════════════╡
    #   # │ 0               ┆ 3               ┆ 0   ┆ ["A", "B", "B"] │
    #   # │ 2               ┆ 5               ┆ 2   ┆ ["B", "B", "C"] │
    #   # │ 4               ┆ 7               ┆ 4   ┆ ["C"]           │
    #   # └─────────────────┴─────────────────┴─────┴─────────────────┘
    def group_by_dynamic(
      index_column,
      every:,
      period: nil,
      offset: nil,
      truncate: nil,
      include_boundaries: false,
      closed: "left",
      label: "left",
      by: nil,
      start_by: "window",
      check_sorted: true
    )
      if !truncate.nil?
        label = truncate ? "left" : "datapoint"
      end

      index_column = Utils.expr_to_lit_or_expr(index_column, str_to_lit: false)
      if offset.nil?
        offset = period.nil? ? "-#{every}" : "0ns"
      end

      if period.nil?
        period = every
      end

      period = Utils._timedelta_to_pl_duration(period)
      offset = Utils._timedelta_to_pl_duration(offset)
      every = Utils._timedelta_to_pl_duration(every)

      rbexprs_by = by.nil? ? [] : Utils.selection_to_rbexpr_list(by)
      lgb = _ldf.group_by_dynamic(
        index_column._rbexpr,
        every,
        period,
        offset,
        label,
        include_boundaries,
        closed,
        rbexprs_by,
        start_by,
        check_sorted
      )
      LazyGroupBy.new(lgb)
    end
    alias_method :groupby_dynamic, :group_by_dynamic

    # Perform an asof join.
    #
    # This is similar to a left-join except that we match on nearest key rather than
    # equal keys.
    #
    # Both DataFrames must be sorted by the join_asof key.
    #
    # For each row in the left DataFrame:
    #
    # - A "backward" search selects the last row in the right DataFrame whose 'on' key is less than or equal to the left's key.
    # - A "forward" search selects the first row in the right DataFrame whose 'on' key is greater than or equal to the left's key.
    #
    # The default is "backward".
    #
    # @param other [LazyFrame]
    #   Lazy DataFrame to join with.
    # @param left_on [String]
    #   Join column of the left DataFrame.
    # @param right_on [String]
    #   Join column of the right DataFrame.
    # @param on [String]
    #   Join column of both DataFrames. If set, `left_on` and `right_on` should be
    #   None.
    # @param by [Object]
    #   Join on these columns before doing asof join.
    # @param by_left [Object]
    #   Join on these columns before doing asof join.
    # @param by_right [Object]
    #   Join on these columns before doing asof join.
    # @param strategy ["backward", "forward"]
    #   Join strategy.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
    # @param tolerance [Object]
    #   Numeric tolerance. By setting this the join will only be done if the near
    #   keys are within this distance. If an asof join is done on columns of dtype
    #   "Date", "Datetime", "Duration" or "Time" you use the following string
    #   language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    #   Or combine them:
    #   "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # @param allow_parallel [Boolean]
    #   Allow the physical plan to optionally evaluate the computation of both
    #   DataFrames up to the join in parallel.
    # @param force_parallel [Boolean]
    #   Force the physical plan to evaluate the computation of both DataFrames up to
    #   the join in parallel.
    #
    # @return [LazyFrame]
    def join_asof(
      other,
      left_on: nil,
      right_on: nil,
      on: nil,
      by_left: nil,
      by_right: nil,
      by: nil,
      strategy: "backward",
      suffix: "_right",
      tolerance: nil,
      allow_parallel: true,
      force_parallel: false
    )
      if !other.is_a?(LazyFrame)
        raise ArgumentError, "Expected a `LazyFrame` as join table, got #{other.class.name}"
      end

      if on.is_a?(String)
        left_on = on
        right_on = on
      end

      if left_on.nil? || right_on.nil?
        raise ArgumentError, "You should pass the column to join on as an argument."
      end

      if by_left.is_a?(String) || by_left.is_a?(Expr)
        by_left_ = [by_left]
      else
        by_left_ = by_left
      end

      if by_right.is_a?(String) || by_right.is_a?(Expr)
        by_right_ = [by_right]
      else
        by_right_ = by_right
      end

      if by.is_a?(String)
        by_left_ = [by]
        by_right_ = [by]
      elsif by.is_a?(::Array)
        by_left_ = by
        by_right_ = by
      end

      tolerance_str = nil
      tolerance_num = nil
      if tolerance.is_a?(String)
        tolerance_str = tolerance
      else
        tolerance_num = tolerance
      end

      _from_rbldf(
        _ldf.join_asof(
          other._ldf,
          Polars.col(left_on)._rbexpr,
          Polars.col(right_on)._rbexpr,
          by_left_,
          by_right_,
          allow_parallel,
          force_parallel,
          suffix,
          strategy,
          tolerance_num,
          tolerance_str
        )
      )
    end

    # Add a join operation to the Logical Plan.
    #
    # @param other [LazyFrame]
    #   Lazy DataFrame to join with.
    # @param left_on [Object]
    #   Join column of the left DataFrame.
    # @param right_on [Object]
    #   Join column of the right DataFrame.
    # @param on Object
    #   Join column of both DataFrames. If set, `left_on` and `right_on` should be
    #   None.
    # @param how ["inner", "left", "outer", "semi", "anti", "cross"]
    #   Join strategy.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
    # @param allow_parallel [Boolean]
    #   Allow the physical plan to optionally evaluate the computation of both
    #   DataFrames up to the join in parallel.
    # @param force_parallel [Boolean]
    #   Force the physical plan to evaluate the computation of both DataFrames up to
    #   the join in parallel.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   other_df = Polars::DataFrame.new(
    #     {
    #       "apple" => ["x", "y", "z"],
    #       "ham" => ["a", "b", "d"]
    #     }
    #   ).lazy
    #   df.join(other_df, on: "ham").collect
    #   # =>
    #   # shape: (2, 4)
    #   # ┌─────┬─────┬─────┬───────┐
    #   # │ foo ┆ bar ┆ ham ┆ apple │
    #   # │ --- ┆ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ f64 ┆ str ┆ str   │
    #   # ╞═════╪═════╪═════╪═══════╡
    #   # │ 1   ┆ 6.0 ┆ a   ┆ x     │
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
    #   # └─────┴─────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "outer").collect
    #   # =>
    #   # shape: (4, 4)
    #   # ┌──────┬──────┬─────┬───────┐
    #   # │ foo  ┆ bar  ┆ ham ┆ apple │
    #   # │ ---  ┆ ---  ┆ --- ┆ ---   │
    #   # │ i64  ┆ f64  ┆ str ┆ str   │
    #   # ╞══════╪══════╪═════╪═══════╡
    #   # │ 1    ┆ 6.0  ┆ a   ┆ x     │
    #   # │ 2    ┆ 7.0  ┆ b   ┆ y     │
    #   # │ null ┆ null ┆ d   ┆ z     │
    #   # │ 3    ┆ 8.0  ┆ c   ┆ null  │
    #   # └──────┴──────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "left").collect
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬─────┬───────┐
    #   # │ foo ┆ bar ┆ ham ┆ apple │
    #   # │ --- ┆ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ f64 ┆ str ┆ str   │
    #   # ╞═════╪═════╪═════╪═══════╡
    #   # │ 1   ┆ 6.0 ┆ a   ┆ x     │
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
    #   # │ 3   ┆ 8.0 ┆ c   ┆ null  │
    #   # └─────┴─────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "semi").collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # └─────┴─────┴─────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "anti").collect
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # └─────┴─────┴─────┘
    def join(
      other,
      left_on: nil,
      right_on: nil,
      on: nil,
      how: "inner",
      suffix: "_right",
      allow_parallel: true,
      force_parallel: false
    )
      if !other.is_a?(LazyFrame)
        raise ArgumentError, "Expected a `LazyFrame` as join table, got #{other.class.name}"
      end

      if how == "cross"
        return _from_rbldf(
          _ldf.join(
            other._ldf, [], [], allow_parallel, force_parallel, how, suffix
          )
        )
      end

      if !on.nil?
        rbexprs = Utils.selection_to_rbexpr_list(on)
        rbexprs_left = rbexprs
        rbexprs_right = rbexprs
      elsif !left_on.nil? && !right_on.nil?
        rbexprs_left = Utils.selection_to_rbexpr_list(left_on)
        rbexprs_right = Utils.selection_to_rbexpr_list(right_on)
      else
        raise ArgumentError, "must specify `on` OR `left_on` and `right_on`"
      end

      _from_rbldf(
        self._ldf.join(
          other._ldf,
          rbexprs_left,
          rbexprs_right,
          allow_parallel,
          force_parallel,
          how,
          suffix,
        )
      )
    end

    # Add or overwrite multiple columns in a DataFrame.
    #
    # @param exprs [Object]
    #   List of Expressions that evaluate to columns.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   ).lazy
    #   ldf.with_columns(
    #     [
    #       (Polars.col("a") ** 2).alias("a^2"),
    #       (Polars.col("b") / 2).alias("b/2"),
    #       (Polars.col("c").is_not).alias("not c")
    #     ]
    #   ).collect
    #   # =>
    #   # shape: (4, 6)
    #   # ┌─────┬──────┬───────┬──────┬──────┬───────┐
    #   # │ a   ┆ b    ┆ c     ┆ a^2  ┆ b/2  ┆ not c │
    #   # │ --- ┆ ---  ┆ ---   ┆ ---  ┆ ---  ┆ ---   │
    #   # │ i64 ┆ f64  ┆ bool  ┆ f64  ┆ f64  ┆ bool  │
    #   # ╞═════╪══════╪═══════╪══════╪══════╪═══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ 1.0  ┆ 0.25 ┆ false │
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 4.0  ┆ 2.0  ┆ false │
    #   # │ 3   ┆ 10.0 ┆ false ┆ 9.0  ┆ 5.0  ┆ true  │
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 16.0 ┆ 6.5  ┆ false │
    #   # └─────┴──────┴───────┴──────┴──────┴───────┘
    def with_columns(exprs)
      exprs =
        if exprs.nil?
          []
        elsif exprs.is_a?(Expr)
          [exprs]
        else
          exprs.to_a
        end

      rbexprs = []
      exprs.each do |e|
        case e
        when Expr
          rbexprs << e._rbexpr
        when Series
          rbexprs << Utils.lit(e)._rbexpr
        else
          raise ArgumentError, "Expected an expression, got #{e}"
        end
      end

      _from_rbldf(_ldf.with_columns(rbexprs))
    end

    # Add an external context to the computation graph.
    #
    # This allows expressions to also access columns from DataFrames
    # that are not part of this one.
    #
    # @param other [Object]
    #   Lazy DataFrame to join with.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df_a = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["a", "c", nil]}).lazy
    #   df_other = Polars::DataFrame.new({"c" => ["foo", "ham"]})
    #   (
    #     df_a.with_context(df_other.lazy).select(
    #       [Polars.col("b") + Polars.col("c").first]
    #     )
    #   ).collect
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ b    │
    #   # │ ---  │
    #   # │ str  │
    #   # ╞══════╡
    #   # │ afoo │
    #   # │ cfoo │
    #   # │ null │
    #   # └──────┘
    def with_context(other)
      if !other.is_a?(::Array)
        other = [other]
      end

      _from_rbldf(_ldf.with_context(other.map(&:_ldf)))
    end

    # Add or overwrite column in a DataFrame.
    #
    # @param column [Object]
    #   Expression that evaluates to column or a Series to use.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.with_column((Polars.col("b") ** 2).alias("b_squared")).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬───────────┐
    #   # │ a   ┆ b   ┆ b_squared │
    #   # │ --- ┆ --- ┆ ---       │
    #   # │ i64 ┆ i64 ┆ f64       │
    #   # ╞═════╪═════╪═══════════╡
    #   # │ 1   ┆ 2   ┆ 4.0       │
    #   # │ 3   ┆ 4   ┆ 16.0      │
    #   # │ 5   ┆ 6   ┆ 36.0      │
    #   # └─────┴─────┴───────────┘
    #
    # @example
    #   df.with_column(Polars.col("a") ** 2).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬─────┐
    #   # │ a    ┆ b   │
    #   # │ ---  ┆ --- │
    #   # │ f64  ┆ i64 │
    #   # ╞══════╪═════╡
    #   # │ 1.0  ┆ 2   │
    #   # │ 9.0  ┆ 4   │
    #   # │ 25.0 ┆ 6   │
    #   # └──────┴─────┘
    def with_column(column)
      with_columns([column])
    end

    # Remove one or multiple columns from a DataFrame.
    #
    # @param columns [Object]
    #   - Name of the column that should be removed.
    #   - List of column names.
    #
    # @return [LazyFrame]
    def drop(columns)
      if columns.is_a?(String)
        columns = [columns]
      end
      _from_rbldf(_ldf.drop_columns(columns))
    end

    # Rename column names.
    #
    # @param mapping [Hash]
    #   Key value pairs that map from old name to new name.
    #
    # @return [LazyFrame]
    def rename(mapping)
      existing = mapping.keys
      _new = mapping.values
      _from_rbldf(_ldf.rename(existing, _new))
    end

    # Reverse the DataFrame.
    #
    # @return [LazyFrame]
    def reverse
      _from_rbldf(_ldf.reverse)
    end

    # Shift the values by a given period.
    #
    # @param n [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #   Fill the resulting null values with this value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.shift(1).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬──────┐
    #   # │ a    ┆ b    │
    #   # │ ---  ┆ ---  │
    #   # │ i64  ┆ i64  │
    #   # ╞══════╪══════╡
    #   # │ null ┆ null │
    #   # │ 1    ┆ 2    │
    #   # │ 3    ┆ 4    │
    #   # └──────┴──────┘
    #
    # @example
    #   df.shift(-1).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬──────┐
    #   # │ a    ┆ b    │
    #   # │ ---  ┆ ---  │
    #   # │ i64  ┆ i64  │
    #   # ╞══════╪══════╡
    #   # │ 3    ┆ 4    │
    #   # │ 5    ┆ 6    │
    #   # │ null ┆ null │
    #   # └──────┴──────┘
    def shift(n, fill_value: nil)
      if !fill_value.nil?
        fill_value = Utils.parse_as_expression(fill_value, str_as_lit: true)
      end
      n = Utils.parse_as_expression(n)
      _from_rbldf(_ldf.shift(n, fill_value))
    end

    # Shift the values by a given period and fill the resulting null values.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #   Fill `nil` values with the result of this expression.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.shift_and_fill(1, 0).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 0   ┆ 0   │
    #   # │ 1   ┆ 2   │
    #   # │ 3   ┆ 4   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.shift_and_fill(-1, 0).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 3   ┆ 4   │
    #   # │ 5   ┆ 6   │
    #   # │ 0   ┆ 0   │
    #   # └─────┴─────┘
    def shift_and_fill(periods, fill_value)
      shift(periods, fill_value: fill_value)
    end

    # Get a slice of this DataFrame.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil`, all rows starting at the offset
    #   will be selected.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["x", "y", "z"],
    #       "b" => [1, 3, 5],
    #       "c" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.slice(1, 2).collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ y   ┆ 3   ┆ 4   │
    #   # │ z   ┆ 5   ┆ 6   │
    #   # └─────┴─────┴─────┘
    def slice(offset, length = nil)
      if length && length < 0
        raise ArgumentError, "Negative slice lengths (#{length}) are invalid for LazyFrame"
      end
      _from_rbldf(_ldf.slice(offset, length))
    end

    # Get the first `n` rows.
    #
    # Alias for {#head}.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [LazyFrame]
    #
    # @note
    #   Consider using the {#fetch} operation if you only want to test your
    #   query. The {#fetch} operation will load the first `n` rows at the scan
    #   level, whereas the {#head}/{#limit} are applied at the end.
    def limit(n = 5)
      head(5)
    end

    # Get the first `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [LazyFrame]
    #
    # @note
    #   Consider using the {#fetch} operation if you only want to test your
    #   query. The {#fetch} operation will load the first `n` rows at the scan
    #   level, whereas the {#head}/{#limit} are applied at the end.
    def head(n = 5)
      slice(0, n)
    end

    # Get the last `n` rows.
    #
    # @param n [Integer]
    #     Number of rows.
    #
    # @return [LazyFrame]
    def tail(n = 5)
      _from_rbldf(_ldf.tail(n))
    end

    # Get the last row of the DataFrame.
    #
    # @return [LazyFrame]
    def last
      tail(1)
    end

    # Get the first row of the DataFrame.
    #
    # @return [LazyFrame]
    def first
      slice(0, 1)
    end

    # Add a column at index 0 that counts the rows.
    #
    # @param name [String]
    #   Name of the column to add.
    # @param offset [Integer]
    #   Start the row count at this offset.
    #
    # @return [LazyFrame]
    #
    # @note
    #   This can have a negative effect on query performance.
    #   This may, for instance, block predicate pushdown optimization.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.with_row_count.collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬─────┬─────┐
    #   # │ row_nr ┆ a   ┆ b   │
    #   # │ ---    ┆ --- ┆ --- │
    #   # │ u32    ┆ i64 ┆ i64 │
    #   # ╞════════╪═════╪═════╡
    #   # │ 0      ┆ 1   ┆ 2   │
    #   # │ 1      ┆ 3   ┆ 4   │
    #   # │ 2      ┆ 5   ┆ 6   │
    #   # └────────┴─────┴─────┘
    def with_row_count(name: "row_nr", offset: 0)
      _from_rbldf(_ldf.with_row_count(name, offset))
    end

    # Take every nth row in the LazyFrame and return as a new LazyFrame.
    #
    # @return [LazyFrame]
    #
    # @example
    #   s = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [5, 6, 7, 8]}).lazy
    #   s.take_every(2).collect
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 5   │
    #   # │ 3   ┆ 7   │
    #   # └─────┴─────┘
    def take_every(n)
      select(Utils.col("*").take_every(n))
    end

    # Fill null values using the specified value or strategy.
    #
    # @return [LazyFrame]
    def fill_null(value = nil, strategy: nil, limit: nil, matches_supertype: nil)
      select(Polars.all.fill_null(value, strategy: strategy, limit: limit))
    end

    # Fill floating point NaN values.
    #
    # @param fill_value [Object]
    #   Value to fill the NaN values with.
    #
    # @return [LazyFrame]
    #
    # @note
    #   Note that floating point NaN (Not a Number) are not missing values!
    #   To replace missing values, use `fill_null` instead.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1.5, 2, Float::NAN, 4],
    #       "b" => [0.5, 4, Float::NAN, 13],
    #     }
    #   ).lazy
    #   df.fill_nan(99).collect
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────┬──────┐
    #   # │ a    ┆ b    │
    #   # │ ---  ┆ ---  │
    #   # │ f64  ┆ f64  │
    #   # ╞══════╪══════╡
    #   # │ 1.5  ┆ 0.5  │
    #   # │ 2.0  ┆ 4.0  │
    #   # │ 99.0 ┆ 99.0 │
    #   # │ 4.0  ┆ 13.0 │
    #   # └──────┴──────┘
    def fill_nan(fill_value)
      if !fill_value.is_a?(Expr)
        fill_value = Utils.lit(fill_value)
      end
      _from_rbldf(_ldf.fill_nan(fill_value._rbexpr))
    end

    # Aggregate the columns in the DataFrame to their standard deviation value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.std.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────────┬─────┐
    #   # │ a        ┆ b   │
    #   # │ ---      ┆ --- │
    #   # │ f64      ┆ f64 │
    #   # ╞══════════╪═════╡
    #   # │ 1.290994 ┆ 0.5 │
    #   # └──────────┴─────┘
    #
    # @example
    #   df.std(ddof: 0).collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────────┬──────────┐
    #   # │ a        ┆ b        │
    #   # │ ---      ┆ ---      │
    #   # │ f64      ┆ f64      │
    #   # ╞══════════╪══════════╡
    #   # │ 1.118034 ┆ 0.433013 │
    #   # └──────────┴──────────┘
    def std(ddof: 1)
      _from_rbldf(_ldf.std(ddof))
    end

    # Aggregate the columns in the DataFrame to their variance value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.var.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────────┬──────┐
    #   # │ a        ┆ b    │
    #   # │ ---      ┆ ---  │
    #   # │ f64      ┆ f64  │
    #   # ╞══════════╪══════╡
    #   # │ 1.666667 ┆ 0.25 │
    #   # └──────────┴──────┘
    #
    # @example
    #   df.var(ddof: 0).collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────┬────────┐
    #   # │ a    ┆ b      │
    #   # │ ---  ┆ ---    │
    #   # │ f64  ┆ f64    │
    #   # ╞══════╪════════╡
    #   # │ 1.25 ┆ 0.1875 │
    #   # └──────┴────────┘
    def var(ddof: 1)
      _from_rbldf(_ldf.var(ddof))
    end

    # Aggregate the columns in the DataFrame to their maximum value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.max.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 4   ┆ 2   │
    #   # └─────┴─────┘
    def max
      _from_rbldf(_ldf.max)
    end

    # Aggregate the columns in the DataFrame to their minimum value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.min.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 1   │
    #   # └─────┴─────┘
    def min
      _from_rbldf(_ldf.min)
    end

    # Aggregate the columns in the DataFrame to their sum value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.sum.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 10  ┆ 5   │
    #   # └─────┴─────┘
    def sum
      _from_rbldf(_ldf.sum)
    end

    # Aggregate the columns in the DataFrame to their mean value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.mean.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ f64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 2.5 ┆ 1.25 │
    #   # └─────┴──────┘
    def mean
      _from_rbldf(_ldf.mean)
    end

    # Aggregate the columns in the DataFrame to their median value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.median.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ f64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 2.5 ┆ 1.0 │
    #   # └─────┴─────┘
    def median
      _from_rbldf(_ldf.median)
    end

    # Aggregate the columns in the DataFrame to their quantile value.
    #
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.quantile(0.7).collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ f64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 3.0 ┆ 1.0 │
    #   # └─────┴─────┘
    def quantile(quantile, interpolation: "nearest")
      quantile = Utils.expr_to_lit_or_expr(quantile, str_to_lit: false)
      _from_rbldf(_ldf.quantile(quantile._rbexpr, interpolation))
    end

    # Explode lists to long format.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["a", "a", "b", "c"],
    #       "numbers" => [[1], [2, 3], [4, 5], [6, 7, 8]],
    #     }
    #   ).lazy
    #   df.explode("numbers").collect
    #   # =>
    #   # shape: (8, 2)
    #   # ┌─────────┬─────────┐
    #   # │ letters ┆ numbers │
    #   # │ ---     ┆ ---     │
    #   # │ str     ┆ i64     │
    #   # ╞═════════╪═════════╡
    #   # │ a       ┆ 1       │
    #   # │ a       ┆ 2       │
    #   # │ a       ┆ 3       │
    #   # │ b       ┆ 4       │
    #   # │ b       ┆ 5       │
    #   # │ c       ┆ 6       │
    #   # │ c       ┆ 7       │
    #   # │ c       ┆ 8       │
    #   # └─────────┴─────────┘
    def explode(columns)
      columns = Utils.selection_to_rbexpr_list(columns)
      _from_rbldf(_ldf.explode(columns))
    end

    # Drop duplicate rows from this DataFrame.
    #
    # Note that this fails if there is a column of type `List` in the DataFrame or
    # subset.
    #
    # @param maintain_order [Boolean]
    #   Keep the same order as the original DataFrame. This requires more work to
    #   compute.
    # @param subset [Object]
    #   Subset to use to compare rows.
    # @param keep ["first", "last"]
    #   Which of the duplicate rows to keep.
    #
    # @return [LazyFrame]
    def unique(maintain_order: true, subset: nil, keep: "first")
      if !subset.nil? && !subset.is_a?(::Array)
        subset = [subset]
      end
      _from_rbldf(_ldf.unique(maintain_order, subset, keep))
    end

    # Drop rows with null values from this LazyFrame.
    #
    # @param subset [Object]
    #   Subset of column(s) on which `drop_nulls` will be applied.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, nil, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.lazy.drop_nulls.collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 3   ┆ 8   ┆ c   │
    #   # └─────┴─────┴─────┘
    def drop_nulls(subset: nil)
      if !subset.nil? && !subset.is_a?(::Array)
        subset = [subset]
      end
      _from_rbldf(_ldf.drop_nulls(subset))
    end

    # Unpivot a DataFrame from wide to long format.
    #
    # Optionally leaves identifiers set.
    #
    # This function is useful to massage a DataFrame into a format where one or more
    # columns are identifier variables (id_vars), while all other columns, considered
    # measured variables (value_vars), are "unpivoted" to the row axis, leaving just
    # two non-identifier columns, 'variable' and 'value'.
    #
    # @param id_vars [Object]
    #   Columns to use as identifier variables.
    # @param value_vars [Object]
    #   Values to use as identifier variables.
    #   If `value_vars` is empty all columns that are not in `id_vars` will be used.
    # @param variable_name [String]
    #   Name to give to the `value` column. Defaults to "variable"
    # @param value_name [String]
    #   Name to give to the `value` column. Defaults to "value"
    # @param streamable [Boolean]
    #   Allow this node to run in the streaming engine.
    #   If this runs in streaming, the output of the melt operation
    #   will not have a stable ordering.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["x", "y", "z"],
    #       "b" => [1, 3, 5],
    #       "c" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.melt(id_vars: "a", value_vars: ["b", "c"]).collect
    #   # =>
    #   # shape: (6, 3)
    #   # ┌─────┬──────────┬───────┐
    #   # │ a   ┆ variable ┆ value │
    #   # │ --- ┆ ---      ┆ ---   │
    #   # │ str ┆ str      ┆ i64   │
    #   # ╞═════╪══════════╪═══════╡
    #   # │ x   ┆ b        ┆ 1     │
    #   # │ y   ┆ b        ┆ 3     │
    #   # │ z   ┆ b        ┆ 5     │
    #   # │ x   ┆ c        ┆ 2     │
    #   # │ y   ┆ c        ┆ 4     │
    #   # │ z   ┆ c        ┆ 6     │
    #   # └─────┴──────────┴───────┘
    def melt(id_vars: nil, value_vars: nil, variable_name: nil, value_name: nil, streamable: true)
      if value_vars.is_a?(String)
        value_vars = [value_vars]
      end
      if id_vars.is_a?(String)
        id_vars = [id_vars]
      end
      if value_vars.nil?
        value_vars = []
      end
      if id_vars.nil?
        id_vars = []
      end
      _from_rbldf(
        _ldf.melt(id_vars, value_vars, value_name, variable_name, streamable)
      )
    end

    # def map
    # end

    # Interpolate intermediate values. The interpolation method is linear.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, nil, 9, 10],
    #       "bar" => [6, 7, 9, nil],
    #       "baz" => [1, nil, nil, 9]
    #     }
    #   ).lazy
    #   df.interpolate.collect
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────┬──────┬──────────┐
    #   # │ foo  ┆ bar  ┆ baz      │
    #   # │ ---  ┆ ---  ┆ ---      │
    #   # │ f64  ┆ f64  ┆ f64      │
    #   # ╞══════╪══════╪══════════╡
    #   # │ 1.0  ┆ 6.0  ┆ 1.0      │
    #   # │ 5.0  ┆ 7.0  ┆ 3.666667 │
    #   # │ 9.0  ┆ 9.0  ┆ 6.333333 │
    #   # │ 10.0 ┆ null ┆ 9.0      │
    #   # └──────┴──────┴──────────┘
    def interpolate
      select(Utils.col("*").interpolate)
    end

    # Decompose a struct into its fields.
    #
    # The fields will be inserted into the `DataFrame` on the location of the
    # `struct` type.
    #
    # @param names [Object]
    #   Names of the struct columns that will be decomposed by its fields
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = (
    #     Polars::DataFrame.new(
    #       {
    #         "before" => ["foo", "bar"],
    #         "t_a" => [1, 2],
    #         "t_b" => ["a", "b"],
    #         "t_c" => [true, nil],
    #         "t_d" => [[1, 2], [3]],
    #         "after" => ["baz", "womp"]
    #       }
    #     )
    #     .lazy
    #     .select(
    #       ["before", Polars.struct(Polars.col("^t_.$")).alias("t_struct"), "after"]
    #     )
    #   )
    #   df.fetch
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────┬─────────────────────┬───────┐
    #   # │ before ┆ t_struct            ┆ after │
    #   # │ ---    ┆ ---                 ┆ ---   │
    #   # │ str    ┆ struct[4]           ┆ str   │
    #   # ╞════════╪═════════════════════╪═══════╡
    #   # │ foo    ┆ {1,"a",true,[1, 2]} ┆ baz   │
    #   # │ bar    ┆ {2,"b",null,[3]}    ┆ womp  │
    #   # └────────┴─────────────────────┴───────┘
    #
    # @example
    #   df.unnest("t_struct").fetch
    #   # =>
    #   # shape: (2, 6)
    #   # ┌────────┬─────┬─────┬──────┬───────────┬───────┐
    #   # │ before ┆ t_a ┆ t_b ┆ t_c  ┆ t_d       ┆ after │
    #   # │ ---    ┆ --- ┆ --- ┆ ---  ┆ ---       ┆ ---   │
    #   # │ str    ┆ i64 ┆ str ┆ bool ┆ list[i64] ┆ str   │
    #   # ╞════════╪═════╪═════╪══════╪═══════════╪═══════╡
    #   # │ foo    ┆ 1   ┆ a   ┆ true ┆ [1, 2]    ┆ baz   │
    #   # │ bar    ┆ 2   ┆ b   ┆ null ┆ [3]       ┆ womp  │
    #   # └────────┴─────┴─────┴──────┴───────────┴───────┘
    def unnest(names)
      if names.is_a?(String)
        names = [names]
      end
      _from_rbldf(_ldf.unnest(names))
    end

    # TODO
    # def merge_sorted
    # end

    # Indicate that one or multiple columns are sorted.
    #
    # @param column [Object]
    #   Columns that are sorted
    # @param more_columns [Object]
    #   Additional columns that are sorted, specified as positional arguments.
    # @param descending [Boolean]
    #   Whether the columns are sorted in descending order.
    #
    # @return [LazyFrame]
    def set_sorted(
      column,
      *more_columns,
      descending: false
    )
      columns = Utils.selection_to_rbexpr_list(column)
      if more_columns.any?
        columns.concat(Utils.selection_to_rbexpr_list(more_columns))
      end
      with_columns(
        columns.map { |e| Utils.wrap_expr(e).set_sorted(descending: descending) }
      )
    end

    # TODO
    # def update
    # end

    private

    def initialize_copy(other)
      super
      self._ldf = _ldf._clone
    end

    def _from_rbldf(rb_ldf)
      self.class._from_rbldf(rb_ldf)
    end
  end
end
