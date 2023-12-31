module Polars
  # Two-dimensional data structure representing data as a table with rows and columns.
  class DataFrame
    include Plot

    # @private
    attr_accessor :_df

    # Create a new DataFrame.
    #
    # @param data [Hash, Array, Series, nil]
    #   Two-dimensional data in various forms. Hash must contain Arrays.
    #   Array may contain Series.
    # @param columns [Array, Hash, nil]
    #   Column labels to use for resulting DataFrame. If specified, overrides any
    #   labels already present in the data. Must match data dimensions.
    # @param orient ["col", "row", nil]
    #   Whether to interpret two-dimensional data as columns or as rows. If `nil`,
    #   the orientation is inferred by matching the columns and data dimensions. If
    #   this does not yield conclusive results, column orientation is used.
    def initialize(data = nil, schema: nil, columns: nil, schema_overrides: nil, orient: nil, infer_schema_length: 100, nan_to_null: false)
      schema ||= columns

      if defined?(ActiveRecord) && (data.is_a?(ActiveRecord::Relation) || data.is_a?(ActiveRecord::Result))
        raise ArgumentError, "Use read_database instead"
      end

      if data.nil?
        self._df = self.class.hash_to_rbdf({}, schema: schema, schema_overrides: schema_overrides)
      elsif data.is_a?(Hash)
        data = data.transform_keys { |v| v.is_a?(Symbol) ? v.to_s : v }
        self._df = self.class.hash_to_rbdf(data, schema: schema, schema_overrides: schema_overrides, nan_to_null: nan_to_null)
      elsif data.is_a?(::Array)
        self._df = self.class.sequence_to_rbdf(data, schema: schema, schema_overrides: schema_overrides, orient: orient, infer_schema_length: infer_schema_length)
      elsif data.is_a?(Series)
        self._df = self.class.series_to_rbdf(data, schema: schema, schema_overrides: schema_overrides)
      else
        raise ArgumentError, "DataFrame constructor called with unsupported type; got #{data.class.name}"
      end
    end

    # @private
    def self._from_rbdf(rb_df)
      df = DataFrame.allocate
      df._df = rb_df
      df
    end

    # @private
    def self._from_hashes(data, infer_schema_length: 100, schema: nil)
      rbdf = RbDataFrame.read_hashes(data, infer_schema_length, schema)
      _from_rbdf(rbdf)
    end

    # @private
    def self._from_hash(data, schema: nil, schema_overrides: nil)
      _from_rbdf(hash_to_rbdf(data, schema: schema, schema_overrides: schema_overrides))
    end

    # def self._from_records
    # end

    # def self._from_numo
    # end

    # no self._from_arrow

    # no self._from_pandas

    # @private
    def self._read_csv(
      file,
      has_header: true,
      columns: nil,
      sep: str = ",",
      comment_char: nil,
      quote_char: '"',
      skip_rows: 0,
      dtypes: nil,
      null_values: nil,
      ignore_errors: false,
      parse_dates: false,
      n_threads: nil,
      infer_schema_length: 100,
      batch_size: 8192,
      n_rows: nil,
      encoding: "utf8",
      low_memory: false,
      rechunk: true,
      skip_rows_after_header: 0,
      row_count_name: nil,
      row_count_offset: 0,
      sample_size: 1024,
      eol_char: "\n"
    )
      if Utils.pathlike?(file)
        path = Utils.normalise_filepath(file)
      else
        path = nil
        # if defined?(StringIO) && file.is_a?(StringIO)
        #   file = file.string
        # end
      end

      dtype_list = nil
      dtype_slice = nil
      if !dtypes.nil?
        if dtypes.is_a?(Hash)
          dtype_list = []
          dtypes.each do|k, v|
            dtype_list << [k, Utils.rb_type_to_dtype(v)]
          end
        elsif dtypes.is_a?(::Array)
          dtype_slice = dtypes
        else
          raise ArgumentError, "dtype arg should be list or dict"
        end
      end

      processed_null_values = Utils._process_null_values(null_values)

      if columns.is_a?(String)
        columns = [columns]
      end
      if file.is_a?(String) && file.include?("*")
        dtypes_dict = nil
        if !dtype_list.nil?
          dtypes_dict = dtype_list.to_h
        end
        if !dtype_slice.nil?
          raise ArgumentError, "cannot use glob patterns and unnamed dtypes as `dtypes` argument; Use dtypes: Mapping[str, Type[DataType]"
        end
        scan = Polars.scan_csv(
          file,
          has_header: has_header,
          sep: sep,
          comment_char: comment_char,
          quote_char: quote_char,
          skip_rows: skip_rows,
          dtypes: dtypes_dict,
          null_values: null_values,
          ignore_errors: ignore_errors,
          infer_schema_length: infer_schema_length,
          n_rows: n_rows,
          low_memory: low_memory,
          rechunk: rechunk,
          skip_rows_after_header: skip_rows_after_header,
          row_count_name: row_count_name,
          row_count_offset: row_count_offset,
          eol_char: eol_char
        )
        if columns.nil?
          return _from_rbdf(scan.collect._df)
        elsif is_str_sequence(columns, allow_str: false)
          return _from_rbdf(scan.select(columns).collect._df)
        else
          raise ArgumentError, "cannot use glob patterns and integer based projection as `columns` argument; Use columns: List[str]"
        end
      end

      projection, columns = Utils.handle_projection_columns(columns)

      _from_rbdf(
        RbDataFrame.read_csv(
          file,
          infer_schema_length,
          batch_size,
          has_header,
          ignore_errors,
          n_rows,
          skip_rows,
          projection,
          sep,
          rechunk,
          columns,
          encoding,
          n_threads,
          path,
          dtype_list,
          dtype_slice,
          low_memory,
          comment_char,
          quote_char,
          processed_null_values,
          parse_dates,
          skip_rows_after_header,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          sample_size,
          eol_char
        )
      )
    end

    # @private
    def self._read_parquet(
      source,
      columns: nil,
      n_rows: nil,
      parallel: "auto",
      row_count_name: nil,
      row_count_offset: 0,
      low_memory: false,
      use_statistics: true,
      rechunk: true,
      storage_options: {},
      hive_partitioning: true
    )
      if Utils.uri?(source)
        source = source.to_s
      elsif Utils.pathlike?(source)
        source = Utils.normalise_filepath(source)
      end
      if columns.is_a?(String)
        columns = [columns]
      end

      if source.is_a?(String) && source.include?("*") && Utils.local_file?(source)
        scan =
          Polars.scan_parquet(
            source,
            n_rows: n_rows,
            rechunk: true,
            parallel: parallel,
            row_count_name: row_count_name,
            row_count_offset: row_count_offset,
            low_memory: low_memory,
            storage_options: storage_options,
            hive_partitioning: hive_partitioning
          )

        if columns.nil?
          return self._from_rbdf(scan.collect._df)
        elsif Utils.is_str_sequence(columns, allow_str: false)
          return self._from_rbdf(scan.select(columns).collect._df)
        else
          raise ArgumentError, "cannot use glob patterns and integer based projection as `columns` argument; Use columns: Array[String]"
        end
      end

      projection, columns = Utils.handle_projection_columns(columns)
      _from_rbdf(
        RbDataFrame.read_parquet(
          source,
          columns,
          projection,
          n_rows,
          parallel,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          low_memory,
          use_statistics,
          rechunk
        )
      )
    end

    # @private
    def self._read_avro(file, columns: nil, n_rows: nil)
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end
      projection, columns = Utils.handle_projection_columns(columns)
      _from_rbdf(RbDataFrame.read_avro(file, columns, projection, n_rows))
    end

    # @private
    def self._read_ipc(
      file,
      columns: nil,
      n_rows: nil,
      row_count_name: nil,
      row_count_offset: 0,
      rechunk: true,
      memory_map: true
    )
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end
      if columns.is_a?(String)
        columns = [columns]
      end

      if file.is_a?(String) && file.include?("*")
        raise Todo
      end

      projection, columns = Utils.handle_projection_columns(columns)
      _from_rbdf(
        RbDataFrame.read_ipc(
          file,
          columns,
          projection,
          n_rows,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          memory_map
        )
      )
    end

    # @private
    def self._read_json(file)
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      _from_rbdf(RbDataFrame.read_json(file))
    end

    # @private
    def self._read_ndjson(file)
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      _from_rbdf(RbDataFrame.read_ndjson(file))
    end

    # Get the shape of the DataFrame.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5]})
    #   df.shape
    #   # => [5, 1]
    def shape
      _df.shape
    end

    # Get the height of the DataFrame.
    #
    # @return [Integer]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5]})
    #   df.height
    #   # => 5
    def height
      _df.height
    end
    alias_method :count, :height
    alias_method :length, :height
    alias_method :size, :height

    # Get the width of the DataFrame.
    #
    # @return [Integer]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5]})
    #   df.width
    #   # => 1
    def width
      _df.width
    end

    # Get column names.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.columns
    #   # => ["foo", "bar", "ham"]
    def columns
      _df.columns
    end

    # Change the column names of the DataFrame.
    #
    # @param columns [Array]
    #   A list with new names for the DataFrame.
    #   The length of the list should be equal to the width of the DataFrame.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.columns = ["apple", "banana", "orange"]
    #   df
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬────────┬────────┐
    #   # │ apple ┆ banana ┆ orange │
    #   # │ ---   ┆ ---    ┆ ---    │
    #   # │ i64   ┆ i64    ┆ str    │
    #   # ╞═══════╪════════╪════════╡
    #   # │ 1     ┆ 6      ┆ a      │
    #   # │ 2     ┆ 7      ┆ b      │
    #   # │ 3     ┆ 8      ┆ c      │
    #   # └───────┴────────┴────────┘
    def columns=(columns)
      _df.set_column_names(columns)
    end

    # Get dtypes of columns in DataFrame. Dtypes can also be found in column headers when printing the DataFrame.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.dtypes
    #   # => [Polars::Int64, Polars::Float64, Polars::Utf8]
    def dtypes
      _df.dtypes
    end

    # Get the schema.
    #
    # @return [Hash]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.schema
    #   # => {"foo"=>Polars::Int64, "bar"=>Polars::Float64, "ham"=>Polars::Utf8}
    def schema
      columns.zip(dtypes).to_h
    end

    # Equal.
    #
    # @return [DataFrame]
    def ==(other)
      _comp(other, "eq")
    end

    # Not equal.
    #
    # @return [DataFrame]
    def !=(other)
      _comp(other, "neq")
    end

    # Greater than.
    #
    # @return [DataFrame]
    def >(other)
      _comp(other, "gt")
    end

    # Less than.
    #
    # @return [DataFrame]
    def <(other)
      _comp(other, "lt")
    end

    # Greater than or equal.
    #
    # @return [DataFrame]
    def >=(other)
      _comp(other, "gt_eq")
    end

    # Less than or equal.
    #
    # @return [DataFrame]
    def <=(other)
      _comp(other, "lt_eq")
    end

    # Performs multiplication.
    #
    # @return [DataFrame]
    def *(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.mul_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.mul(other._s))
    end

    # Performs division.
    #
    # @return [DataFrame]
    def /(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.div_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.div(other._s))
    end

    # Performs addition.
    #
    # @return [DataFrame]
    def +(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.add_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.add(other._s))
    end

    # Performs subtraction.
    #
    # @return [DataFrame]
    def -(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.sub_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.sub(other._s))
    end

    # Returns the modulo.
    #
    # @return [DataFrame]
    def %(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.rem_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.rem(other._s))
    end

    # Returns a string representing the DataFrame.
    #
    # @return [String]
    def to_s
      _df.to_s
    end
    alias_method :inspect, :to_s

    # Returns an array representing the DataFrame
    #
    # @return [Array]
    def to_a
      rows(named: true)
    end

    # Check if DataFrame includes column.
    #
    # @return [Boolean]
    def include?(name)
      columns.include?(name)
    end

    # Returns an enumerator.
    #
    # @return [Object]
    def each(&block)
      get_columns.each(&block)
    end

    # Returns subset of the DataFrame.
    #
    # @return [Object]
    def [](*args)
      if args.size == 2
        row_selection, col_selection = args

        # df[.., unknown]
        if row_selection.is_a?(Range)

          # multiple slices
          # df[.., ..]
          if col_selection.is_a?(Range)
            raise Todo
          end
        end

        # df[2, ..] (select row as df)
        if row_selection.is_a?(Integer)
          if col_selection.is_a?(::Array)
            df = self[0.., col_selection]
            return df.slice(row_selection, 1)
          end
          # df[2, "a"]
          if col_selection.is_a?(String) || col_selection.is_a?(Symbol)
            return self[col_selection][row_selection]
          end
        end

        # column selection can be "a" and ["a", "b"]
        if col_selection.is_a?(String) || col_selection.is_a?(Symbol)
          col_selection = [col_selection]
        end

        # df[.., 1]
        if col_selection.is_a?(Integer)
          series = to_series(col_selection)
          return series[row_selection]
        end

        if col_selection.is_a?(::Array)
          # df[.., [1, 2]]
          if Utils.is_int_sequence(col_selection)
            series_list = col_selection.map { |i| to_series(i) }
            df = self.class.new(series_list)
            return df[row_selection]
          end
        end

        df = self[col_selection]
        return df[row_selection]
      elsif args.size == 1
        item = args[0]

        # select single column
        # df["foo"]
        if item.is_a?(String) || item.is_a?(Symbol)
          return Utils.wrap_s(_df.column(item.to_s))
        end

        # df[idx]
        if item.is_a?(Integer)
          return slice(_pos_idx(item, 0), 1)
        end

        # df[..]
        if item.is_a?(Range)
          return Slice.new(self).apply(item)
        end

        if item.is_a?(::Array) && item.all? { |v| Utils.strlike?(v) }
          # select multiple columns
          # df[["foo", "bar"]]
          return _from_rbdf(_df.select(item.map(&:to_s)))
        end

        if Utils.is_int_sequence(item)
          item = Series.new("", item)
        end

        if item.is_a?(Series)
          dtype = item.dtype
          if dtype == Utf8
            return _from_rbdf(_df.select(item))
          elsif dtype == UInt32
            return _from_rbdf(_df.take_with_series(item._s))
          elsif [UInt8, UInt16, UInt64, Int8, Int16, Int32, Int64].include?(dtype)
            return _from_rbdf(
              _df.take_with_series(_pos_idxs(item, 0)._s)
            )
          end
        end
      end

      # Ruby-specific
      if item.is_a?(Expr) || item.is_a?(Series)
        return filter(item)
      end

      raise ArgumentError, "Cannot get item of type: #{item.class.name}"
    end

    # Set item.
    #
    # @return [Object]
    def []=(*key, value)
      if key.length == 1
        key = key.first
      elsif key.length != 2
        raise ArgumentError, "wrong number of arguments (given #{key.length + 1}, expected 2..3)"
      end

      if Utils.strlike?(key)
        if value.is_a?(::Array) || (defined?(Numo::NArray) && value.is_a?(Numo::NArray))
          value = Series.new(value)
        elsif !value.is_a?(Series)
          value = Polars.lit(value)
        end
        self._df = with_column(value.alias(key.to_s))._df
      elsif key.is_a?(::Array)
        row_selection, col_selection = key

        if Utils.strlike?(col_selection)
          s = self[col_selection]
        elsif col_selection.is_a?(Integer)
          raise Todo
        else
          raise ArgumentError, "column selection not understood: #{col_selection}"
        end

        s[row_selection] = value

        if col_selection.is_a?(Integer)
          replace_at_idx(col_selection, s)
        elsif Utils.strlike?(col_selection)
          replace(col_selection, s)
        end
      else
        raise Todo
      end
    end

    # Return the dataframe as a scalar.
    #
    # Equivalent to `df[0,0]`, with a check that the shape is (1,1).
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    #   result = df.select((Polars.col("a") * Polars.col("b")).sum)
    #   result.item
    #   # => 32
    def item
      if shape != [1, 1]
        raise ArgumentError, "Can only call .item if the dataframe is of shape (1,1), dataframe is of shape #{shape}"
      end
      self[0, 0]
    end

    # no to_arrow

    # Convert DataFrame to a hash mapping column name to values.
    #
    # @return [Hash]
    def to_h(as_series: true)
      if as_series
        get_columns.to_h { |s| [s.name, s] }
      else
        get_columns.to_h { |s| [s.name, s.to_a] }
      end
    end

    # Convert every row to a dictionary.
    #
    # Note that this is slow.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   df.to_hashes
    #   # =>
    #   # [{"foo"=>1, "bar"=>4}, {"foo"=>2, "bar"=>5}, {"foo"=>3, "bar"=>6}]
    def to_hashes
      rbdf = _df
      names = columns

      height.times.map do |i|
        names.zip(rbdf.row_tuple(i)).to_h
      end
    end

    # Convert DataFrame to a 2D Numo array.
    #
    # This operation clones data.
    #
    # @return [Numo::NArray]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"foo" => [1, 2, 3], "bar" => [6, 7, 8], "ham" => ["a", "b", "c"]}
    #   )
    #   df.to_numo.class
    #   # => Numo::RObject
    def to_numo
      out = _df.to_numo
      if out.nil?
        Numo::NArray.vstack(width.times.map { |i| to_series(i).to_numo }).transpose
      else
        out
      end
    end

    # no to_pandas

    # Select column as Series at index location.
    #
    # @param index [Integer]
    #   Location of selection.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.to_series(1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'bar' [i64]
    #   # [
    #   #         6
    #   #         7
    #   #         8
    #   # ]
    def to_series(index = 0)
      if index < 0
        index = columns.length + index
      end
      Utils.wrap_s(_df.select_at_idx(index))
    end

    # Serialize to JSON representation.
    #
    # @return [nil]
    #
    # @param file [String]
    #   File path to which the result should be written.
    # @param pretty [Boolean]
    #   Pretty serialize json.
    # @param row_oriented [Boolean]
    #   Write to row oriented json. This is slower, but more common.
    #
    # @see #write_ndjson
    def write_json(
      file,
      pretty: false,
      row_oriented: false
    )
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      _df.write_json(file, pretty, row_oriented)
      nil
    end

    # Serialize to newline delimited JSON representation.
    #
    # @param file [String]
    #   File path to which the result should be written.
    #
    # @return [nil]
    def write_ndjson(file)
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      _df.write_ndjson(file)
      nil
    end

    # Write to comma-separated values (CSV) file.
    #
    # @param file [String, nil]
    #   File path to which the result should be written. If set to `nil`
    #   (default), the output is returned as a string instead.
    # @param has_header [Boolean]
    #   Whether to include header in the CSV output.
    # @param sep [String]
    #   Separate CSV fields with this symbol.
    # @param quote [String]
    #   Byte to use as quoting character.
    # @param batch_size [Integer]
    #   Number of rows that will be processed per thread.
    # @param datetime_format [String, nil]
    #   A format string, with the specifiers defined by the
    #   [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   Rust crate. If no format specified, the default fractional-second
    #   precision is inferred from the maximum timeunit found in the frame's
    #   Datetime cols (if any).
    # @param date_format [String, nil]
    #   A format string, with the specifiers defined by the
    #   [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   Rust crate.
    # @param time_format [String, nil]
    #   A format string, with the specifiers defined by the
    #   [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   Rust crate.
    # @param float_precision [Integer, nil]
    #   Number of decimal places to write, applied to both `:f32` and
    #   `:f64` datatypes.
    # @param null_value [String, nil]
    #   A string representing null values (defaulting to the empty string).
    #
    # @return [String, nil]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3, 4, 5],
    #       "bar" => [6, 7, 8, 9, 10],
    #       "ham" => ["a", "b", "c", "d", "e"]
    #     }
    #   )
    #   df.write_csv("file.csv")
    def write_csv(
      file = nil,
      has_header: true,
      include_header: nil,
      sep: ",",
      quote: '"',
      batch_size: 1024,
      datetime_format: nil,
      date_format: nil,
      time_format: nil,
      float_precision: nil,
      null_value: nil
    )
      include_header = has_header if include_header.nil?

      if sep.length > 1
        raise ArgumentError, "only single byte separator is allowed"
      elsif quote.length > 1
        raise ArgumentError, "only single byte quote char is allowed"
      elsif null_value == ""
        null_value = nil
      end

      if file.nil?
        buffer = StringIO.new
        buffer.set_encoding(Encoding::BINARY)
        _df.write_csv(
          buffer,
          include_header,
          sep.ord,
          quote.ord,
          batch_size,
          datetime_format,
          date_format,
          time_format,
          float_precision,
          null_value
        )
        return buffer.string.force_encoding(Encoding::UTF_8)
      end

      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      _df.write_csv(
        file,
        include_header,
        sep.ord,
        quote.ord,
        batch_size,
        datetime_format,
        date_format,
        time_format,
        float_precision,
        null_value,
      )
      nil
    end

    # Write to comma-separated values (CSV) string.
    #
    # @return [String]
    def to_csv(**options)
      write_csv(**options)
    end

    # Write to Apache Avro file.
    #
    # @param file [String]
    #   File path to which the file should be written.
    # @param compression ["uncompressed", "snappy", "deflate"]
    #   Compression method. Defaults to "uncompressed".
    #
    # @return [nil]
    def write_avro(file, compression = "uncompressed")
      if compression.nil?
        compression = "uncompressed"
      end
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      _df.write_avro(file, compression)
    end

    # Write to Arrow IPC binary stream or Feather file.
    #
    # @param file [String]
    #   File path to which the file should be written.
    # @param compression ["uncompressed", "lz4", "zstd"]
    #   Compression method. Defaults to "uncompressed".
    #
    # @return [nil]
    def write_ipc(file, compression: "uncompressed")
      return_bytes = file.nil?
      if return_bytes
        file = StringIO.new
        file.set_encoding(Encoding::BINARY)
      end
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      if compression.nil?
        compression = "uncompressed"
      end

      _df.write_ipc(file, compression)
      return_bytes ? file.string : nil
    end

    # Write to Apache Parquet file.
    #
    # @param file [String]
    #   File path to which the file should be written.
    # @param compression ["lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"]
    #   Choose "zstd" for good compression performance.
    #   Choose "lz4" for fast compression/decompression.
    #   Choose "snappy" for more backwards compatibility guarantees
    #   when you deal with older parquet readers.
    # @param compression_level [Integer, nil]
    #   The level of compression to use. Higher compression means smaller files on
    #   disk.
    #
    #   - "gzip" : min-level: 0, max-level: 10.
    #   - "brotli" : min-level: 0, max-level: 11.
    #   - "zstd" : min-level: 1, max-level: 22.
    # @param statistics [Boolean]
    #   Write statistics to the parquet headers. This requires extra compute.
    # @param row_group_size [Integer, nil]
    #   Size of the row groups in number of rows.
    #   If `nil` (default), the chunks of the DataFrame are
    #   used. Writing in smaller chunks may reduce memory pressure and improve
    #   writing speeds.
    #
    # @return [nil]
    def write_parquet(
      file,
      compression: "zstd",
      compression_level: nil,
      statistics: false,
      row_group_size: nil
    )
      if compression.nil?
        compression = "uncompressed"
      end
      if Utils.pathlike?(file)
        file = Utils.normalise_filepath(file)
      end

      _df.write_parquet(
        file, compression, compression_level, statistics, row_group_size
      )
    end

    # Return an estimation of the total (heap) allocated size of the DataFrame.
    #
    # Estimated size is given in the specified unit (bytes by default).
    #
    # This estimation is the sum of the size of its buffers, validity, including
    # nested arrays. Multiple arrays may share buffers and bitmaps. Therefore, the
    # size of 2 arrays is not the sum of the sizes computed from this function. In
    # particular, StructArray's size is an upper bound.
    #
    # When an array is sliced, its allocated size remains constant because the buffer
    # unchanged. However, this function will yield a smaller number. This is because
    # this function returns the visible size of the buffer, not its total capacity.
    #
    # FFI buffers are included in this estimation.
    #
    # @param unit ["b", "kb", "mb", "gb", "tb"]
    #   Scale the returned size to the given unit.
    #
    # @return [Numeric]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "x" => 1_000_000.times.to_a.reverse,
    #       "y" => 1_000_000.times.map { |v| v / 1000.0 },
    #       "z" => 1_000_000.times.map(&:to_s)
    #     },
    #     columns: {"x" => :u32, "y" => :f64, "z" => :str}
    #   )
    #   df.estimated_size
    #   # => 25888898
    #   df.estimated_size("mb")
    #   # => 24.689577102661133
    def estimated_size(unit = "b")
      sz = _df.estimated_size
      Utils.scale_bytes(sz, to: unit)
    end

    # Transpose a DataFrame over the diagonal.
    #
    # @param include_header [Boolean]
    #   If set, the column names will be added as first column.
    # @param header_name [String]
    #   If `include_header` is set, this determines the name of the column that will
    #   be inserted.
    # @param column_names [Array]
    #   Optional generator/iterator that yields column names. Will be used to
    #   replace the columns in the DataFrame.
    #
    # @return [DataFrame]
    #
    # @note
    #   This is a very expensive operation. Perhaps you can do it differently.
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [1, 2, 3]})
    #   df.transpose(include_header: true)
    #   # =>
    #   # shape: (2, 4)
    #   # ┌────────┬──────────┬──────────┬──────────┐
    #   # │ column ┆ column_0 ┆ column_1 ┆ column_2 │
    #   # │ ---    ┆ ---      ┆ ---      ┆ ---      │
    #   # │ str    ┆ i64      ┆ i64      ┆ i64      │
    #   # ╞════════╪══════════╪══════════╪══════════╡
    #   # │ a      ┆ 1        ┆ 2        ┆ 3        │
    #   # │ b      ┆ 1        ┆ 2        ┆ 3        │
    #   # └────────┴──────────┴──────────┴──────────┘
    #
    # @example Replace the auto-generated column names with a list
    #   df.transpose(include_header: false, column_names: ["a", "b", "c"])
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 2   ┆ 3   │
    #   # │ 1   ┆ 2   ┆ 3   │
    #   # └─────┴─────┴─────┘
    #
    # @example Include the header as a separate column
    #   df.transpose(
    #     include_header: true, header_name: "foo", column_names: ["a", "b", "c"]
    #   )
    #   # =>
    #   # shape: (2, 4)
    #   # ┌─────┬─────┬─────┬─────┐
    #   # │ foo ┆ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╪═════╡
    #   # │ a   ┆ 1   ┆ 2   ┆ 3   │
    #   # │ b   ┆ 1   ┆ 2   ┆ 3   │
    #   # └─────┴─────┴─────┴─────┘
    def transpose(include_header: false, header_name: "column", column_names: nil)
      keep_names_as = include_header ? header_name : nil
      _from_rbdf(_df.transpose(keep_names_as, column_names))
    end

    # Reverse the DataFrame.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "key" => ["a", "b", "c"],
    #       "val" => [1, 2, 3]
    #     }
    #   )
    #   df.reverse
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ key ┆ val │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ c   ┆ 3   │
    #   # │ b   ┆ 2   │
    #   # │ a   ┆ 1   │
    #   # └─────┴─────┘
    def reverse
      select(Polars.col("*").reverse)
    end

    # Rename column names.
    #
    # @param mapping [Hash]
    #   Key value pairs that map from old name to new name.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.rename({"foo" => "apple"})
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬─────┬─────┐
    #   # │ apple ┆ bar ┆ ham │
    #   # │ ---   ┆ --- ┆ --- │
    #   # │ i64   ┆ i64 ┆ str │
    #   # ╞═══════╪═════╪═════╡
    #   # │ 1     ┆ 6   ┆ a   │
    #   # │ 2     ┆ 7   ┆ b   │
    #   # │ 3     ┆ 8   ┆ c   │
    #   # └───────┴─────┴─────┘
    def rename(mapping)
      lazy.rename(mapping).collect(no_optimization: true)
    end

    # Insert a Series at a certain column index. This operation is in place.
    #
    # @param index [Integer]
    #   Column to insert the new `Series` column.
    # @param series [Series]
    #   `Series` to insert.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   s = Polars::Series.new("baz", [97, 98, 99])
    #   df.insert_at_idx(1, s)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ baz ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 97  ┆ 4   │
    #   # │ 2   ┆ 98  ┆ 5   │
    #   # │ 3   ┆ 99  ┆ 6   │
    #   # └─────┴─────┴─────┘
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   )
    #   s = Polars::Series.new("d", [-2.5, 15, 20.5, 0])
    #   df.insert_at_idx(3, s)
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────┬──────┬───────┬──────┐
    #   # │ a   ┆ b    ┆ c     ┆ d    │
    #   # │ --- ┆ ---  ┆ ---   ┆ ---  │
    #   # │ i64 ┆ f64  ┆ bool  ┆ f64  │
    #   # ╞═════╪══════╪═══════╪══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ -2.5 │
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 15.0 │
    #   # │ 3   ┆ 10.0 ┆ false ┆ 20.5 │
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 0.0  │
    #   # └─────┴──────┴───────┴──────┘
    def insert_at_idx(index, series)
      if index < 0
        index = columns.length + index
      end
      _df.insert_at_idx(index, series._s)
      self
    end

    # Filter the rows in the DataFrame based on a predicate expression.
    #
    # @param predicate [Expr]
    #   Expression that evaluates to a boolean Series.
    #
    # @return [DataFrame]
    #
    # @example Filter on one condition:
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.filter(Polars.col("foo") < 3)
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
    #   df.filter((Polars.col("foo") < 3) & (Polars.col("ham") == "a"))
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
      lazy.filter(predicate).collect
    end

    # Summary statistics for a DataFrame.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1.0, 2.8, 3.0],
    #       "b" => [4, 5, nil],
    #       "c" => [true, false, true],
    #       "d" => [nil, "b", "c"],
    #       "e" => ["usd", "eur", nil]
    #     }
    #   )
    #   df.describe
    #   # =>
    #   # shape: (7, 6)
    #   # ┌────────────┬──────────┬──────────┬──────────┬──────┬──────┐
    #   # │ describe   ┆ a        ┆ b        ┆ c        ┆ d    ┆ e    │
    #   # │ ---        ┆ ---      ┆ ---      ┆ ---      ┆ ---  ┆ ---  │
    #   # │ str        ┆ f64      ┆ f64      ┆ f64      ┆ str  ┆ str  │
    #   # ╞════════════╪══════════╪══════════╪══════════╪══════╪══════╡
    #   # │ count      ┆ 3.0      ┆ 3.0      ┆ 3.0      ┆ 3    ┆ 3    │
    #   # │ null_count ┆ 0.0      ┆ 1.0      ┆ 0.0      ┆ 1    ┆ 1    │
    #   # │ mean       ┆ 2.266667 ┆ 4.5      ┆ 0.666667 ┆ null ┆ null │
    #   # │ std        ┆ 1.101514 ┆ 0.707107 ┆ 0.57735  ┆ null ┆ null │
    #   # │ min        ┆ 1.0      ┆ 4.0      ┆ 0.0      ┆ b    ┆ eur  │
    #   # │ max        ┆ 3.0      ┆ 5.0      ┆ 1.0      ┆ c    ┆ usd  │
    #   # │ median     ┆ 2.8      ┆ 4.5      ┆ 1.0      ┆ null ┆ null │
    #   # └────────────┴──────────┴──────────┴──────────┴──────┴──────┘
    def describe
      describe_cast = lambda do |stat|
        columns = []
        self.columns.each_with_index do |s, i|
          if self[s].is_numeric || self[s].is_boolean
            columns << stat[0.., i].cast(:f64)
          else
            # for dates, strings, etc, we cast to string so that all
            # statistics can be shown
            columns << stat[0.., i].cast(:str)
          end
        end
        self.class.new(columns)
      end

      summary = _from_rbdf(
        Polars.concat(
          [
            describe_cast.(
              self.class.new(columns.to_h { |c| [c, [height]] })
            ),
            describe_cast.(null_count),
            describe_cast.(mean),
            describe_cast.(std),
            describe_cast.(min),
            describe_cast.(max),
            describe_cast.(median)
          ]
        )._df
      )
      summary.insert_at_idx(
        0,
        Polars::Series.new(
          "describe",
          ["count", "null_count", "mean", "std", "min", "max", "median"],
        )
      )
      summary
    end

    # Find the index of a column by name.
    #
    # @param name [String]
    #   Name of the column to find.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"foo" => [1, 2, 3], "bar" => [6, 7, 8], "ham" => ["a", "b", "c"]}
    #   )
    #   df.find_idx_by_name("ham")
    #   # => 2
    def find_idx_by_name(name)
      _df.find_idx_by_name(name)
    end

    # Replace a column at an index location.
    #
    # @param index [Integer]
    #   Column index.
    # @param series [Series]
    #   Series that will replace the column.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   s = Polars::Series.new("apple", [10, 20, 30])
    #   df.replace_at_idx(0, s)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬─────┬─────┐
    #   # │ apple ┆ bar ┆ ham │
    #   # │ ---   ┆ --- ┆ --- │
    #   # │ i64   ┆ i64 ┆ str │
    #   # ╞═══════╪═════╪═════╡
    #   # │ 10    ┆ 6   ┆ a   │
    #   # │ 20    ┆ 7   ┆ b   │
    #   # │ 30    ┆ 8   ┆ c   │
    #   # └───────┴─────┴─────┘
    def replace_at_idx(index, series)
      if index < 0
        index = columns.length + index
      end
      _df.replace_at_idx(index, series._s)
      self
    end

    # Sort the DataFrame by column.
    #
    # @param by [String]
    #   By which column to sort.
    # @param reverse [Boolean]
    #   Reverse/descending sort.
    # @param nulls_last [Boolean]
    #   Place null values last. Can only be used if sorted by a single column.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.sort("foo", reverse: true)
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
    #
    # @example Sort by multiple columns.
    #   df.sort(
    #     [Polars.col("foo"), Polars.col("bar")**2],
    #     reverse: [true, false]
    #   )
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
    def sort(by, reverse: false, nulls_last: false)
      lazy
        .sort(by, reverse: reverse, nulls_last: nulls_last)
        .collect(no_optimization: true)
    end

    # Sort the DataFrame by column in-place.
    #
    # @param by [String]
    #   By which column to sort.
    # @param reverse [Boolean]
    #   Reverse/descending sort.
    # @param nulls_last [Boolean]
    #   Place null values last. Can only be used if sorted by a single column.
    #
    # @return [DataFrame]
    def sort!(by, reverse: false, nulls_last: false)
      self._df = sort(by, reverse: reverse, nulls_last: nulls_last)._df
    end

    # Check if DataFrame is equal to other.
    #
    # @param other [DataFrame]
    #   DataFrame to compare with.
    # @param null_equal [Boolean]
    #   Consider null values as equal.
    #
    # @return [Boolean]
    #
    # @example
    #   df1 = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df2 = Polars::DataFrame.new(
    #     {
    #       "foo" => [3, 2, 1],
    #       "bar" => [8.0, 7.0, 6.0],
    #       "ham" => ["c", "b", "a"]
    #     }
    #   )
    #   df1.frame_equal(df1)
    #   # => true
    #   df1.frame_equal(df2)
    #   # => false
    def frame_equal(other, null_equal: true)
      _df.frame_equal(other._df, null_equal)
    end

    # Replace a column by a new Series.
    #
    # @param column [String]
    #   Column to replace.
    # @param new_col [Series]
    #   New column to insert.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   s = Polars::Series.new([10, 20, 30])
    #   df.replace("foo", s)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 10  ┆ 4   │
    #   # │ 20  ┆ 5   │
    #   # │ 30  ┆ 6   │
    #   # └─────┴─────┘
    def replace(column, new_col)
      _df.replace(column.to_s, new_col._s)
      self
    end

    # Get a slice of this DataFrame.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer, nil]
    #   Length of the slice. If set to `nil`, all rows starting at the offset
    #   will be selected.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.slice(1, 2)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # └─────┴─────┴─────┘
    def slice(offset, length = nil)
      if !length.nil? && length < 0
        length = height - offset + length
      end
      _from_rbdf(_df.slice(offset, length))
    end

    # Get the first `n` rows.
    #
    # Alias for {#head}.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"foo" => [1, 2, 3, 4, 5, 6], "bar" => ["a", "b", "c", "d", "e", "f"]}
    #   )
    #   df.limit(4)
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ str │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ a   │
    #   # │ 2   ┆ b   │
    #   # │ 3   ┆ c   │
    #   # │ 4   ┆ d   │
    #   # └─────┴─────┘
    def limit(n = 5)
      head(n)
    end

    # Get the first `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3, 4, 5],
    #       "bar" => [6, 7, 8, 9, 10],
    #       "ham" => ["a", "b", "c", "d", "e"]
    #     }
    #   )
    #   df.head(3)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # │ 3   ┆ 8   ┆ c   │
    #   # └─────┴─────┴─────┘
    def head(n = 5)
      _from_rbdf(_df.head(n))
    end

    # Get the last `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3, 4, 5],
    #       "bar" => [6, 7, 8, 9, 10],
    #       "ham" => ["a", "b", "c", "d", "e"]
    #     }
    #   )
    #   df.tail(3)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8   ┆ c   │
    #   # │ 4   ┆ 9   ┆ d   │
    #   # │ 5   ┆ 10  ┆ e   │
    #   # └─────┴─────┴─────┘
    def tail(n = 5)
      _from_rbdf(_df.tail(n))
    end

    # Return a new DataFrame where the null values are dropped.
    #
    # @param subset [Object]
    #   Subset of column(s) on which `drop_nulls` will be applied.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, nil, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.drop_nulls
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
      if subset.is_a?(String)
        subset = [subset]
      end
      _from_rbdf(_df.drop_nulls(subset))
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
    # @return [Object]
    #
    # @note
    #   It is recommended to use LazyFrame when piping operations, in order
    #   to fully take advantage of query optimization and parallelization.
    #   See {#lazy}.
    #
    # @example
    #   cast_str_to_int = lambda do |data, col_name:|
    #     data.with_column(Polars.col(col_name).cast(:i64))
    #   end
    #
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => ["10", "20", "30", "40"]})
    #   df.pipe(cast_str_to_int, col_name: "b")
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

    # Add a column at index 0 that counts the rows.
    #
    # @param name [String]
    #   Name of the column to add.
    # @param offset [Integer]
    #   Start the row count at this offset.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.with_row_count
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
      _from_rbdf(_df.with_row_count(name, offset))
    end

    # Start a group by operation.
    #
    # @param by [Object]
    #   Column(s) to group by.
    # @param maintain_order [Boolean]
    #   Make sure that the order of the groups remain consistent. This is more
    #   expensive than a default group by. Note that this only works in expression
    #   aggregations.
    #
    # @return [GroupBy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [1, 2, 3, 4, 5, 6],
    #       "c" => [6, 5, 4, 3, 2, 1]
    #     }
    #   )
    #   df.group_by("a").agg(Polars.col("b").sum).sort("a")
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
      if !Utils.bool?(maintain_order)
        raise TypeError, "invalid input for group_by arg `maintain_order`: #{maintain_order}."
      end
      GroupBy.new(
        self,
        by,
        maintain_order: maintain_order
      )
    end
    alias_method :groupby, :group_by
    alias_method :group, :group_by

    # Create rolling groups based on a time column.
    #
    # Also works for index values of type `:i32` or `:i64`.
    #
    # Different from a `dynamic_group_by` the windows are now determined by the
    # individual values and are not of constant intervals. For constant intervals use
    # *group_by_dynamic*
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
    # - **"1i"      # length 1**
    # - **"10i"     # length 10**
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
    # @return [RollingGroupBy]
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
    #   df = Polars::DataFrame.new({"dt" => dates, "a" => [3, 7, 5, 9, 2, 1]}).with_column(
    #     Polars.col("dt").str.strptime(Polars::Datetime).set_sorted
    #   )
    #   df.group_by_rolling(index_column: "dt", period: "2d").agg(
    #     [
    #       Polars.sum("a").alias("sum_a"),
    #       Polars.min("a").alias("min_a"),
    #       Polars.max("a").alias("max_a")
    #     ]
    #   )
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
      RollingGroupBy.new(self, index_column, period, offset, closed, by, check_sorted)
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
    # @param index_column
    #   Column used to group based on the time window.
    #   Often to type Date/Datetime
    #   This column must be sorted in ascending order. If not the output will not
    #   make sense.
    #
    #   In case of a dynamic group by on indices, dtype needs to be one of
    #   `:i32`, `:i64`. Note that `:i32` gets temporarily cast to `:i64`, so if
    #   performance matters use an `:i64` column.
    # @param every
    #   Interval of the window.
    # @param period
    #   Length of the window, if None it is equal to 'every'.
    # @param offset
    #   Offset of the window if None and period is None it will be equal to negative
    #   `every`.
    # @param truncate
    #   Truncate the time value to the window lower bound.
    # @param include_boundaries
    #   Add the lower and upper bound of the window to the "_lower_bound" and
    #   "_upper_bound" columns. This will impact performance because it's harder to
    #   parallelize
    # @param closed ["right", "left", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    # @param by
    #   Also group by this column/these columns
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
      truncate: true,
      include_boundaries: false,
      closed: "left",
      by: nil,
      start_by: "window"
    )
      DynamicGroupBy.new(
        self,
        index_column,
        every,
        period,
        offset,
        truncate,
        include_boundaries,
        closed,
        by,
        start_by
      )
    end
    alias_method :groupby_dynamic, :group_by_dynamic

    # Upsample a DataFrame at a regular frequency.
    #
    # @param time_column [Object]
    #   time column will be used to determine a date_range.
    #   Note that this column has to be sorted for the output to make sense.
    # @param every [String]
    #   interval will start 'every' duration
    # @param offset [String]
    #   change the start of the date_range by this offset.
    # @param by [Object]
    #   First group by these columns and then upsample for every group
    # @param maintain_order [Boolean]
    #   Keep the ordering predictable. This is slower.
    #
    # The `every` and `offset` arguments are created with
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
    # @return [DataFrame]
    #
    # @example Upsample a DataFrame by a certain interval.
    #   df = Polars::DataFrame.new(
    #     {
    #       "time" => [
    #         DateTime.new(2021, 2, 1),
    #         DateTime.new(2021, 4, 1),
    #         DateTime.new(2021, 5, 1),
    #         DateTime.new(2021, 6, 1)
    #       ],
    #       "groups" => ["A", "B", "A", "B"],
    #       "values" => [0, 1, 2, 3]
    #     }
    #   ).set_sorted("time")
    #   df.upsample(
    #     time_column: "time", every: "1mo", by: "groups", maintain_order: true
    #   ).select(Polars.all.forward_fill)
    #   # =>
    #   # shape: (7, 3)
    #   # ┌─────────────────────┬────────┬────────┐
    #   # │ time                ┆ groups ┆ values │
    #   # │ ---                 ┆ ---    ┆ ---    │
    #   # │ datetime[ns]        ┆ str    ┆ i64    │
    #   # ╞═════════════════════╪════════╪════════╡
    #   # │ 2021-02-01 00:00:00 ┆ A      ┆ 0      │
    #   # │ 2021-03-01 00:00:00 ┆ A      ┆ 0      │
    #   # │ 2021-04-01 00:00:00 ┆ A      ┆ 0      │
    #   # │ 2021-05-01 00:00:00 ┆ A      ┆ 2      │
    #   # │ 2021-04-01 00:00:00 ┆ B      ┆ 1      │
    #   # │ 2021-05-01 00:00:00 ┆ B      ┆ 1      │
    #   # │ 2021-06-01 00:00:00 ┆ B      ┆ 3      │
    #   # └─────────────────────┴────────┴────────┘
    def upsample(
      time_column:,
      every:,
      offset: nil,
      by: nil,
      maintain_order: false
    )
      if by.nil?
        by = []
      end
      if by.is_a?(String)
        by = [by]
      end
      if offset.nil?
        offset = "0ns"
      end

      every = Utils._timedelta_to_pl_duration(every)
      offset = Utils._timedelta_to_pl_duration(offset)

      _from_rbdf(
        _df.upsample(by, time_column, every, offset, maintain_order)
      )
    end

    # Perform an asof join.
    #
    # This is similar to a left-join except that we match on nearest key rather than
    # equal keys.
    #
    # Both DataFrames must be sorted by the asof_join key.
    #
    # For each row in the left DataFrame:
    #
    # - A "backward" search selects the last row in the right DataFrame whose 'on' key is less than or equal to the left's key.
    # - A "forward" search selects the first row in the right DataFrame whose 'on' key is greater than or equal to the left's key.
    #
    # The default is "backward".
    #
    # @param other [DataFrame]
    #   DataFrame to join with.
    # @param left_on [String]
    #   Join column of the left DataFrame.
    # @param right_on [String]
    #   Join column of the right DataFrame.
    # @param on [String]
    #   Join column of both DataFrames. If set, `left_on` and `right_on` should be
    #   None.
    # @param by [Object]
    #   join on these columns before doing asof join
    # @param by_left [Object]
    #   join on these columns before doing asof join
    # @param by_right [Object]
    #   join on these columns before doing asof join
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
    #    - 1ns   (1 nanosecond)
    #    - 1us   (1 microsecond)
    #    - 1ms   (1 millisecond)
    #    - 1s    (1 second)
    #    - 1m    (1 minute)
    #    - 1h    (1 hour)
    #    - 1d    (1 day)
    #    - 1w    (1 week)
    #    - 1mo   (1 calendar month)
    #    - 1y    (1 calendar year)
    #    - 1i    (1 index count)
    #
    #    Or combine them:
    #    "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # @param allow_parallel [Boolean]
    #   Allow the physical plan to optionally evaluate the computation of both
    #   DataFrames up to the join in parallel.
    # @param force_parallel [Boolean]
    #   Force the physical plan to evaluate the computation of both DataFrames up to
    #   the join in parallel.
    #
    # @return [DataFrame]
    #
    # @example
    #   gdp = Polars::DataFrame.new(
    #     {
    #       "date" => [
    #         DateTime.new(2016, 1, 1),
    #         DateTime.new(2017, 1, 1),
    #         DateTime.new(2018, 1, 1),
    #         DateTime.new(2019, 1, 1),
    #       ],  # note record date: Jan 1st (sorted!)
    #       "gdp" => [4164, 4411, 4566, 4696]
    #     }
    #   ).set_sorted("date")
    #   population = Polars::DataFrame.new(
    #     {
    #       "date" => [
    #         DateTime.new(2016, 5, 12),
    #         DateTime.new(2017, 5, 12),
    #         DateTime.new(2018, 5, 12),
    #         DateTime.new(2019, 5, 12),
    #       ],  # note record date: May 12th (sorted!)
    #       "population" => [82.19, 82.66, 83.12, 83.52]
    #     }
    #   ).set_sorted("date")
    #   population.join_asof(
    #     gdp, left_on: "date", right_on: "date", strategy: "backward"
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────────────┬────────────┬──────┐
    #   # │ date                ┆ population ┆ gdp  │
    #   # │ ---                 ┆ ---        ┆ ---  │
    #   # │ datetime[ns]        ┆ f64        ┆ i64  │
    #   # ╞═════════════════════╪════════════╪══════╡
    #   # │ 2016-05-12 00:00:00 ┆ 82.19      ┆ 4164 │
    #   # │ 2017-05-12 00:00:00 ┆ 82.66      ┆ 4411 │
    #   # │ 2018-05-12 00:00:00 ┆ 83.12      ┆ 4566 │
    #   # │ 2019-05-12 00:00:00 ┆ 83.52      ┆ 4696 │
    #   # └─────────────────────┴────────────┴──────┘
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
      lazy
        .join_asof(
          other.lazy,
          left_on: left_on,
          right_on: right_on,
          on: on,
          by_left: by_left,
          by_right: by_right,
          by: by,
          strategy: strategy,
          suffix: suffix,
          tolerance: tolerance,
          allow_parallel: allow_parallel,
          force_parallel: force_parallel
        )
        .collect(no_optimization: true)
    end

    # Join in SQL-like fashion.
    #
    # @param other [DataFrame]
    #   DataFrame to join with.
    # @param left_on [Object]
    #   Name(s) of the left join column(s).
    # @param right_on [Object]
    #   Name(s) of the right join column(s).
    # @param on [Object]
    #   Name(s) of the join columns in both DataFrames.
    # @param how ["inner", "left", "outer", "semi", "anti", "cross"]
    #   Join strategy.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   other_df = Polars::DataFrame.new(
    #     {
    #       "apple" => ["x", "y", "z"],
    #       "ham" => ["a", "b", "d"]
    #     }
    #   )
    #   df.join(other_df, on: "ham")
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
    #   df.join(other_df, on: "ham", how: "outer")
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
    #   df.join(other_df, on: "ham", how: "left")
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
    #   df.join(other_df, on: "ham", how: "semi")
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
    #   df.join(other_df, on: "ham", how: "anti")
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # └─────┴─────┴─────┘
    def join(other, left_on: nil, right_on: nil, on: nil, how: "inner", suffix: "_right")
      lazy
        .join(
          other.lazy,
          left_on: left_on,
          right_on: right_on,
          on: on,
          how: how,
          suffix: suffix,
        )
        .collect(no_optimization: true)
    end

    # Apply a custom/user-defined function (UDF) over the rows of the DataFrame.
    #
    # The UDF will receive each row as a tuple of values: `udf(row)`.
    #
    # Implementing logic using a Ruby function is almost always _significantly_
    # slower and more memory intensive than implementing the same logic using
    # the native expression API because:
    #
    # - The native expression engine runs in Rust; UDFs run in Ruby.
    # - Use of Ruby UDFs forces the DataFrame to be materialized in memory.
    # - Polars-native expressions can be parallelised (UDFs cannot).
    # - Polars-native expressions can be logically optimised (UDFs cannot).
    #
    # Wherever possible you should strongly prefer the native expression API
    # to achieve the best performance.
    #
    # @param return_dtype [Symbol]
    #   Output type of the operation. If none given, Polars tries to infer the type.
    # @param inference_size [Integer]
    #   Only used in the case when the custom function returns rows.
    #   This uses the first `n` rows to determine the output schema
    #
    # @return [Object]
    #
    # @note
    #   The frame-level `apply` cannot track column names (as the UDF is a black-box
    #   that may arbitrarily drop, rearrange, transform, or add new columns); if you
    #   want to apply a UDF such that column names are preserved, you should use the
    #   expression-level `apply` syntax instead.
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [-1, 5, 8]})
    #
    # @example Return a DataFrame by mapping each row to a tuple:
    #   df.apply { |t| [t[0] * 2, t[1] * 3] }
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────────┬──────────┐
    #   # │ column_0 ┆ column_1 │
    #   # │ ---      ┆ ---      │
    #   # │ i64      ┆ i64      │
    #   # ╞══════════╪══════════╡
    #   # │ 2        ┆ -3       │
    #   # │ 4        ┆ 15       │
    #   # │ 6        ┆ 24       │
    #   # └──────────┴──────────┘
    #
    # @example Return a Series by mapping each row to a scalar:
    #   df.apply { |t| t[0] * 2 + t[1] }
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ apply │
    #   # │ ---   │
    #   # │ i64   │
    #   # ╞═══════╡
    #   # │ 1     │
    #   # │ 9     │
    #   # │ 14    │
    #   # └───────┘
    def apply(return_dtype: nil, inference_size: 256, &f)
      out, is_df = _df.apply(f, return_dtype, inference_size)
      if is_df
        _from_rbdf(out)
      else
        _from_rbdf(Utils.wrap_s(out).to_frame._df)
      end
    end

    # Return a new DataFrame with the column added or replaced.
    #
    # @param column [Object]
    #   Series, where the name of the Series refers to the column in the DataFrame.
    #
    # @return [DataFrame]
    #
    # @example Added
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.with_column((Polars.col("b") ** 2).alias("b_squared"))
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
    # @example Replaced
    #   df.with_column(Polars.col("a") ** 2)
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
      lazy
        .with_column(column)
        .collect(no_optimization: true, string_cache: false)
    end

    # Return a new DataFrame grown horizontally by stacking multiple Series to it.
    #
    # @param columns [Object]
    #   Series to stack.
    # @param in_place [Boolean]
    #   Modify in place.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   x = Polars::Series.new("apple", [10, 20, 30])
    #   df.hstack([x])
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬─────┬───────┐
    #   # │ foo ┆ bar ┆ ham ┆ apple │
    #   # │ --- ┆ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ i64 ┆ str ┆ i64   │
    #   # ╞═════╪═════╪═════╪═══════╡
    #   # │ 1   ┆ 6   ┆ a   ┆ 10    │
    #   # │ 2   ┆ 7   ┆ b   ┆ 20    │
    #   # │ 3   ┆ 8   ┆ c   ┆ 30    │
    #   # └─────┴─────┴─────┴───────┘
    def hstack(columns, in_place: false)
      if !columns.is_a?(::Array)
        columns = columns.get_columns
      end
      if in_place
        _df.hstack_mut(columns.map(&:_s))
        self
      else
        _from_rbdf(_df.hstack(columns.map(&:_s)))
      end
    end

    # Grow this DataFrame vertically by stacking a DataFrame to it.
    #
    # @param df [DataFrame]
    #   DataFrame to stack.
    # @param in_place [Boolean]
    #   Modify in place
    #
    # @return [DataFrame]
    #
    # @example
    #   df1 = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2],
    #       "bar" => [6, 7],
    #       "ham" => ["a", "b"]
    #     }
    #   )
    #   df2 = Polars::DataFrame.new(
    #     {
    #       "foo" => [3, 4],
    #       "bar" => [8, 9],
    #       "ham" => ["c", "d"]
    #     }
    #   )
    #   df1.vstack(df2)
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # │ 3   ┆ 8   ┆ c   │
    #   # │ 4   ┆ 9   ┆ d   │
    #   # └─────┴─────┴─────┘
    def vstack(df, in_place: false)
      if in_place
        _df.vstack_mut(df._df)
        self
      else
        _from_rbdf(_df.vstack(df._df))
      end
    end

    # Extend the memory backed by this `DataFrame` with the values from `other`.
    #
    # Different from `vstack` which adds the chunks from `other` to the chunks of this
    # `DataFrame` `extend` appends the data from `other` to the underlying memory
    # locations and thus may cause a reallocation.
    #
    # If this does not cause a reallocation, the resulting data structure will not
    # have any extra chunks and thus will yield faster queries.
    #
    # Prefer `extend` over `vstack` when you want to do a query after a single append.
    # For instance during online operations where you add `n` rows and rerun a query.
    #
    # Prefer `vstack` over `extend` when you want to append many times before doing a
    # query. For instance when you read in multiple files and when to store them in a
    # single `DataFrame`. In the latter case, finish the sequence of `vstack`
    # operations with a `rechunk`.
    #
    # @param other [DataFrame]
    #   DataFrame to vertically add.
    #
    # @return [DataFrame]
    #
    # @example
    #   df1 = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   df2 = Polars::DataFrame.new({"foo" => [10, 20, 30], "bar" => [40, 50, 60]})
    #   df1.extend(df2)
    #   # =>
    #   # shape: (6, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 4   │
    #   # │ 2   ┆ 5   │
    #   # │ 3   ┆ 6   │
    #   # │ 10  ┆ 40  │
    #   # │ 20  ┆ 50  │
    #   # │ 30  ┆ 60  │
    #   # └─────┴─────┘
    def extend(other)
      _df.extend(other._df)
      self
    end

    # Remove column from DataFrame and return as new.
    #
    # @param columns [Object]
    #   Column(s) to drop.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.drop("ham")
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 6.0 │
    #   # │ 2   ┆ 7.0 │
    #   # │ 3   ┆ 8.0 │
    #   # └─────┴─────┘
    def drop(columns)
      if columns.is_a?(::Array)
        df = clone
        columns.each do |n|
          df._df.drop_in_place(n)
        end
        df
      else
        _from_rbdf(_df.drop(columns))
      end
    end

    # Drop in place.
    #
    # @param name [Object]
    #   Column to drop.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.drop_in_place("ham")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'ham' [str]
    #   # [
    #   #         "a"
    #   #         "b"
    #   #         "c"
    #   # ]
    def drop_in_place(name)
      Utils.wrap_s(_df.drop_in_place(name))
    end

    # Drop in place if exists.
    #
    # @param name [Object]
    #   Column to drop.
    #
    # @return [Series]
    def delete(name)
      drop_in_place(name) if include?(name)
    end

    # Create an empty copy of the current DataFrame.
    #
    # Returns a DataFrame with identical schema but no data.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [nil, 2, 3, 4],
    #       "b" => [0.5, nil, 2.5, 13],
    #       "c" => [true, true, false, nil]
    #     }
    #   )
    #   df.cleared
    #   # =>
    #   # shape: (0, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ a   ┆ b   ┆ c    │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ f64 ┆ bool │
    #   # ╞═════╪═════╪══════╡
    #   # └─────┴─────┴──────┘
    def cleared
      height > 0 ? head(0) : clone
    end

    # clone handled by initialize_copy

    # Get the DataFrame as a Array of Series.
    #
    # @return [Array]
    def get_columns
      _df.get_columns.map { |s| Utils.wrap_s(s) }
    end

    # Get a single column as Series by name.
    #
    # @param name [String]
    #   Name of the column to retrieve.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   df.get_column("foo")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'foo' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   # ]
    def get_column(name)
      self[name]
    end

    # Fill null values using the specified value or strategy.
    #
    # @param value [Numeric]
    #   Value used to fill null values.
    # @param strategy [nil, "forward", "backward", "min", "max", "mean", "zero", "one"]
    #   Strategy used to fill null values.
    # @param limit [Integer]
    #   Number of consecutive null values to fill when using the 'forward' or
    #   'backward' strategy.
    # @param matches_supertype [Boolean]
    #   Fill all matching supertype of the fill `value`.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil, 4],
    #       "b" => [0.5, 4, nil, 13]
    #     }
    #   )
    #   df.fill_null(99)
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 99  ┆ 99.0 │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    #
    # @example
    #   df.fill_null(strategy: "forward")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    #
    # @example
    #   df.fill_null(strategy: "max")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 4   ┆ 13.0 │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    #
    # @example
    #   df.fill_null(strategy: "zero")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 0   ┆ 0.0  │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    def fill_null(value = nil, strategy: nil, limit: nil, matches_supertype: true)
      _from_rbdf(
        lazy
          .fill_null(value, strategy: strategy, limit: limit, matches_supertype: matches_supertype)
          .collect(no_optimization: true)
          ._df
      )
    end

    # Fill floating point NaN values by an Expression evaluation.
    #
    # @param fill_value [Object]
    #   Value to fill NaN with.
    #
    # @return [DataFrame]
    #
    # @note
    #   Note that floating point NaNs (Not a Number) are not missing values!
    #   To replace missing values, use `fill_null`.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1.5, 2, Float::NAN, 4],
    #       "b" => [0.5, 4, Float::NAN, 13]
    #     }
    #   )
    #   df.fill_nan(99)
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
      lazy.fill_nan(fill_value).collect(no_optimization: true)
    end

    # Explode `DataFrame` to long format by exploding a column with Lists.
    #
    # @param columns [Object]
    #   Column of LargeList type.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["a", "a", "b", "c"],
    #       "numbers" => [[1], [2, 3], [4, 5], [6, 7, 8]]
    #     }
    #   )
    #   df.explode("numbers")
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
      lazy.explode(columns).collect(no_optimization: true)
    end

    # Create a spreadsheet-style pivot table as a DataFrame.
    #
    # @param values [Object]
    #   Column values to aggregate. Can be multiple columns if the *columns*
    #   arguments contains multiple columns as well
    # @param index [Object]
    #   One or multiple keys to group by
    # @param columns [Object]
    #   Columns whose values will be used as the header of the output DataFrame
    # @param aggregate_fn ["first", "sum", "max", "min", "mean", "median", "last", "count"]
    #   A predefined aggregate function str or an expression.
    # @param maintain_order [Object]
    #   Sort the grouped keys so that the output order is predictable.
    # @param sort_columns [Object]
    #   Sort the transposed columns by name. Default is by order of discovery.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["one", "one", "one", "two", "two", "two"],
    #       "bar" => ["A", "B", "C", "A", "B", "C"],
    #       "baz" => [1, 2, 3, 4, 5, 6]
    #     }
    #   )
    #   df.pivot(values: "baz", index: "foo", columns: "bar")
    #   # =>
    #   # shape: (2, 4)
    #   # ┌─────┬─────┬─────┬─────┐
    #   # │ foo ┆ A   ┆ B   ┆ C   │
    #   # │ --- ┆ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╪═════╡
    #   # │ one ┆ 1   ┆ 2   ┆ 3   │
    #   # │ two ┆ 4   ┆ 5   ┆ 6   │
    #   # └─────┴─────┴─────┴─────┘
    def pivot(
      values:,
      index:,
      columns:,
      aggregate_fn: "first",
      maintain_order: true,
      sort_columns: false,
      separator: "_"
    )
      if values.is_a?(String)
        values = [values]
      end
      if index.is_a?(String)
        index = [index]
      end
      if columns.is_a?(String)
        columns = [columns]
      end

      if aggregate_fn.is_a?(String)
        case aggregate_fn
        when "first"
          aggregate_expr = Polars.element.first._rbexpr
        when "sum"
          aggregate_expr = Polars.element.sum._rbexpr
        when "max"
          aggregate_expr = Polars.element.max._rbexpr
        when "min"
          aggregate_expr = Polars.element.min._rbexpr
        when "mean"
          aggregate_expr = Polars.element.mean._rbexpr
        when "median"
          aggregate_expr = Polars.element.median._rbexpr
        when "last"
          aggregate_expr = Polars.element.last._rbexpr
        when "count"
          aggregate_expr = Polars.count._rbexpr
        else
          raise ArgumentError, "Argument aggregate fn: '#{aggregate_fn}' was not expected."
        end
      elsif aggregate_fn.nil?
        aggregate_expr = nil
      else
        aggregate_expr = aggregate_function._rbexpr
      end

      _from_rbdf(
        _df.pivot_expr(
          values,
          index,
          columns,
          maintain_order,
          sort_columns,
          aggregate_expr,
          separator
        )
      )
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
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["x", "y", "z"],
    #       "b" => [1, 3, 5],
    #       "c" => [2, 4, 6]
    #     }
    #   )
    #   df.melt(id_vars: "a", value_vars: ["b", "c"])
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
    def melt(id_vars: nil, value_vars: nil, variable_name: nil, value_name: nil)
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
      _from_rbdf(
        _df.melt(id_vars, value_vars, value_name, variable_name)
      )
    end

    # Unstack a long table to a wide form without doing an aggregation.
    #
    # This can be much faster than a pivot, because it can skip the grouping phase.
    #
    # @note
    #   This functionality is experimental and may be subject to changes
    #   without it being considered a breaking change.
    #
    # @param step Integer
    #   Number of rows in the unstacked frame.
    # @param how ["vertical", "horizontal"]
    #   Direction of the unstack.
    # @param columns [Object]
    #   Column to include in the operation.
    # @param fill_values [Object]
    #   Fill values that don't fit the new size with this value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "col1" => "A".."I",
    #       "col2" => Polars.arange(0, 9, eager: true)
    #     }
    #   )
    #   # =>
    #   # shape: (9, 2)
    #   # ┌──────┬──────┐
    #   # │ col1 ┆ col2 │
    #   # │ ---  ┆ ---  │
    #   # │ str  ┆ i64  │
    #   # ╞══════╪══════╡
    #   # │ A    ┆ 0    │
    #   # │ B    ┆ 1    │
    #   # │ C    ┆ 2    │
    #   # │ D    ┆ 3    │
    #   # │ E    ┆ 4    │
    #   # │ F    ┆ 5    │
    #   # │ G    ┆ 6    │
    #   # │ H    ┆ 7    │
    #   # │ I    ┆ 8    │
    #   # └──────┴──────┘
    #
    # @example
    #   df.unstack(step: 3, how: "vertical")
    #   # =>
    #   # shape: (3, 6)
    #   # ┌────────┬────────┬────────┬────────┬────────┬────────┐
    #   # │ col1_0 ┆ col1_1 ┆ col1_2 ┆ col2_0 ┆ col2_1 ┆ col2_2 │
    #   # │ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
    #   # │ str    ┆ str    ┆ str    ┆ i64    ┆ i64    ┆ i64    │
    #   # ╞════════╪════════╪════════╪════════╪════════╪════════╡
    #   # │ A      ┆ D      ┆ G      ┆ 0      ┆ 3      ┆ 6      │
    #   # │ B      ┆ E      ┆ H      ┆ 1      ┆ 4      ┆ 7      │
    #   # │ C      ┆ F      ┆ I      ┆ 2      ┆ 5      ┆ 8      │
    #   # └────────┴────────┴────────┴────────┴────────┴────────┘
    #
    # @example
    #   df.unstack(step: 3, how: "horizontal")
    #   # =>
    #   # shape: (3, 6)
    #   # ┌────────┬────────┬────────┬────────┬────────┬────────┐
    #   # │ col1_0 ┆ col1_1 ┆ col1_2 ┆ col2_0 ┆ col2_1 ┆ col2_2 │
    #   # │ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
    #   # │ str    ┆ str    ┆ str    ┆ i64    ┆ i64    ┆ i64    │
    #   # ╞════════╪════════╪════════╪════════╪════════╪════════╡
    #   # │ A      ┆ B      ┆ C      ┆ 0      ┆ 1      ┆ 2      │
    #   # │ D      ┆ E      ┆ F      ┆ 3      ┆ 4      ┆ 5      │
    #   # │ G      ┆ H      ┆ I      ┆ 6      ┆ 7      ┆ 8      │
    #   # └────────┴────────┴────────┴────────┴────────┴────────┘
    def unstack(step:, how: "vertical", columns: nil, fill_values: nil)
      if !columns.nil?
        df = select(columns)
      else
        df = self
      end

      height = df.height
      if how == "vertical"
        n_rows = step
        n_cols = (height / n_rows.to_f).ceil
      else
        n_cols = step
        n_rows = (height / n_cols.to_f).ceil
      end

      n_fill = n_cols * n_rows - height

      if n_fill > 0
        if !fill_values.is_a?(::Array)
          fill_values = [fill_values] * df.width
        end

        df = df.select(
          df.get_columns.zip(fill_values).map do |s, next_fill|
            s.extend_constant(next_fill, n_fill)
          end
        )
      end

      if how == "horizontal"
        df = (
          df.with_column(
            (Polars.arange(0, n_cols * n_rows, eager: true) % n_cols).alias(
              "__sort_order"
            )
          )
          .sort("__sort_order")
          .drop("__sort_order")
        )
      end

      zfill_val = Math.log10(n_cols).floor + 1
      slices =
        df.get_columns.flat_map do |s|
          n_cols.times.map do |slice_nbr|
            s.slice(slice_nbr * n_rows, n_rows).alias("%s_%0#{zfill_val}d" % [s.name, slice_nbr])
          end
        end

      _from_rbdf(DataFrame.new(slices)._df)
    end

    # Split into multiple DataFrames partitioned by groups.
    #
    # @param groups [Object]
    #   Groups to partition by.
    # @param maintain_order [Boolean]
    #   Keep predictable output order. This is slower as it requires an extra sort
    #   operation.
    # @param as_dict [Boolean]
    #   If true, return the partitions in a dictionary keyed by the distinct group
    #   values instead of a list.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["A", "A", "B", "B", "C"],
    #       "N" => [1, 2, 2, 4, 2],
    #       "bar" => ["k", "l", "m", "m", "l"]
    #     }
    #   )
    #   df.partition_by("foo", maintain_order: true)
    #   # =>
    #   # [shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ A   ┆ 1   ┆ k   │
    #   # │ A   ┆ 2   ┆ l   │
    #   # └─────┴─────┴─────┘, shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ B   ┆ 2   ┆ m   │
    #   # │ B   ┆ 4   ┆ m   │
    #   # └─────┴─────┴─────┘, shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ C   ┆ 2   ┆ l   │
    #   # └─────┴─────┴─────┘]
    #
    # @example
    #   df.partition_by("foo", maintain_order: true, as_dict: true)
    #   # =>
    #   # {"A"=>shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ A   ┆ 1   ┆ k   │
    #   # │ A   ┆ 2   ┆ l   │
    #   # └─────┴─────┴─────┘, "B"=>shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ B   ┆ 2   ┆ m   │
    #   # │ B   ┆ 4   ┆ m   │
    #   # └─────┴─────┴─────┘, "C"=>shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ C   ┆ 2   ┆ l   │
    #   # └─────┴─────┴─────┘}
    def partition_by(groups, maintain_order: true, include_key: true, as_dict: false)
      if groups.is_a?(String)
        groups = [groups]
      elsif !groups.is_a?(::Array)
        groups = Array(groups)
      end

      if as_dict
        out = {}
        if groups.length == 1
          _df.partition_by(groups, maintain_order, include_key).each do |df|
            df = _from_rbdf(df)
            out[df[groups][0, 0]] = df
          end
        else
          _df.partition_by(groups, maintain_order, include_key).each do |df|
            df = _from_rbdf(df)
            out[df[groups].row(0)] = df
          end
        end
        out
      else
        _df.partition_by(groups, maintain_order, include_key).map { |df| _from_rbdf(df) }
      end
    end

    # Shift values by the given period.
    #
    # @param n [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #  Fill the resulting null values with this value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.shift(1)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ null ┆ null ┆ null │
    #   # │ 1    ┆ 6    ┆ a    │
    #   # │ 2    ┆ 7    ┆ b    │
    #   # └──────┴──────┴──────┘
    #
    # @example
    #   df.shift(-1)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ 2    ┆ 7    ┆ b    │
    #   # │ 3    ┆ 8    ┆ c    │
    #   # │ null ┆ null ┆ null │
    #   # └──────┴──────┴──────┘
    def shift(n, fill_value: nil)
      lazy.shift(n, fill_value: fill_value).collect(_eager: true)
    end

    # Shift the values by a given period and fill the resulting null values.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #   fill nil values with this value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.shift_and_fill(1, 0)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 0   ┆ 0   ┆ 0   │
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # └─────┴─────┴─────┘
    def shift_and_fill(periods, fill_value)
      shift(periods, fill_value: fill_value)
    end

    # Get a mask of all duplicated rows in this DataFrame.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 1],
    #       "b" => ["x", "y", "z", "x"],
    #     }
    #   )
    #   df.is_duplicated
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         false
    #   #         true
    #   # ]
    def is_duplicated
      Utils.wrap_s(_df.is_duplicated)
    end

    # Get a mask of all unique rows in this DataFrame.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 1],
    #       "b" => ["x", "y", "z", "x"]
    #     }
    #   )
    #   df.is_unique
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         true
    #   #         false
    #   # ]
    def is_unique
      Utils.wrap_s(_df.is_unique)
    end

    # Start a lazy query from this point.
    #
    # @return [LazyFrame]
    def lazy
      wrap_ldf(_df.lazy)
    end

    # Select columns from this DataFrame.
    #
    # @param exprs [Object]
    #   Column or columns to select.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.select("foo")
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
    #   df.select(["foo", "bar"])
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
    #   df.select(Polars.col("foo") + 1)
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
    #   df.select([Polars.col("foo") + 1, Polars.col("bar") + 1])
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
    #   df.select(Polars.when(Polars.col("foo") > 2).then(10).otherwise(0))
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
      _from_rbdf(
        lazy
          .select(exprs)
          .collect(no_optimization: true, string_cache: false)
          ._df
      )
    end

    # Add or overwrite multiple columns in a DataFrame.
    #
    # @param exprs [Array]
    #   Array of Expressions that evaluate to columns.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   )
    #   df.with_columns(
    #     [
    #       (Polars.col("a") ** 2).alias("a^2"),
    #       (Polars.col("b") / 2).alias("b/2"),
    #       (Polars.col("c").is_not).alias("not c")
    #     ]
    #   )
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
      if !exprs.nil? && !exprs.is_a?(::Array)
        exprs = [exprs]
      end
      lazy
        .with_columns(exprs)
        .collect(no_optimization: true, string_cache: false)
    end

    # Get number of chunks used by the ChunkedArrays of this DataFrame.
    #
    # @param strategy ["first", "all"]
    #   Return the number of chunks of the 'first' column,
    #   or 'all' columns in this DataFrame.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   )
    #   df.n_chunks
    #   # => 1
    #   df.n_chunks(strategy: "all")
    #   # => [1, 1, 1]
    def n_chunks(strategy: "first")
      if strategy == "first"
        _df.n_chunks
      elsif strategy == "all"
        get_columns.map(&:n_chunks)
      else
        raise ArgumentError, "Strategy: '{strategy}' not understood. Choose one of {{'first',  'all'}}"
      end
    end

    # Aggregate the columns of this DataFrame to their maximum value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.max
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8   ┆ c   │
    #   # └─────┴─────┴─────┘
    def max(axis: 0)
      if axis == 0
        _from_rbdf(_df.max)
      elsif axis == 1
        Utils.wrap_s(_df.max_horizontal)
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
    end

    # Aggregate the columns of this DataFrame to their minimum value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.min
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # └─────┴─────┴─────┘
    def min(axis: 0)
      if axis == 0
        _from_rbdf(_df.min)
      elsif axis == 1
        Utils.wrap_s(_df.min_horizontal)
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
    end

    # Aggregate the columns of this DataFrame to their sum value.
    #
    # @param axis [Integer]
    #   Either 0 or 1.
    # @param null_strategy ["ignore", "propagate"]
    #   This argument is only used if axis == 1.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"],
    #     }
    #   )
    #   df.sum
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ i64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 6   ┆ 21  ┆ null │
    #   # └─────┴─────┴──────┘
    #
    # @example
    #   df.sum(axis: 1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'foo' [str]
    #   # [
    #   #         "16a"
    #   #         "27b"
    #   #         "38c"
    #   # ]
    def sum(axis: 0, null_strategy: "ignore")
      case axis
      when 0
        _from_rbdf(_df.sum)
      when 1
        Utils.wrap_s(_df.sum_horizontal(null_strategy))
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
    end

    # Aggregate the columns of this DataFrame to their mean value.
    #
    # @param axis [Integer]
    #   Either 0 or 1.
    # @param null_strategy ["ignore", "propagate"]
    #   This argument is only used if axis == 1.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.mean
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 2.0 ┆ 7.0 ┆ null │
    #   # └─────┴─────┴──────┘
    def mean(axis: 0, null_strategy: "ignore")
      case axis
      when 0
        _from_rbdf(_df.mean)
      when 1
        Utils.wrap_s(_df.mean_horizontal(null_strategy))
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
    end

    # Aggregate the columns of this DataFrame to their standard deviation value.
    #
    # @param ddof [Integer]
    #   Degrees of freedom
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.std
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 1.0 ┆ 1.0 ┆ null │
    #   # └─────┴─────┴──────┘
    #
    # @example
    #   df.std(ddof: 0)
    #   # =>
    #   # shape: (1, 3)
    #   # ┌──────────┬──────────┬──────┐
    #   # │ foo      ┆ bar      ┆ ham  │
    #   # │ ---      ┆ ---      ┆ ---  │
    #   # │ f64      ┆ f64      ┆ str  │
    #   # ╞══════════╪══════════╪══════╡
    #   # │ 0.816497 ┆ 0.816497 ┆ null │
    #   # └──────────┴──────────┴──────┘
    def std(ddof: 1)
      _from_rbdf(_df.std(ddof))
    end

    # Aggregate the columns of this DataFrame to their variance value.
    #
    # @param ddof [Integer]
    #   Degrees of freedom
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.var
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 1.0 ┆ 1.0 ┆ null │
    #   # └─────┴─────┴──────┘
    #
    # @example
    #   df.var(ddof: 0)
    #   # =>
    #   # shape: (1, 3)
    #   # ┌──────────┬──────────┬──────┐
    #   # │ foo      ┆ bar      ┆ ham  │
    #   # │ ---      ┆ ---      ┆ ---  │
    #   # │ f64      ┆ f64      ┆ str  │
    #   # ╞══════════╪══════════╪══════╡
    #   # │ 0.666667 ┆ 0.666667 ┆ null │
    #   # └──────────┴──────────┴──────┘
    def var(ddof: 1)
      _from_rbdf(_df.var(ddof))
    end

    # Aggregate the columns of this DataFrame to their median value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.median
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 2.0 ┆ 7.0 ┆ null │
    #   # └─────┴─────┴──────┘
    def median
      _from_rbdf(_df.median)
    end

    # Aggregate the columns of this DataFrame to their product values.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => [0.5, 4, 10],
    #       "c" => [true, true, false]
    #     }
    #   )
    #   df.product
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬──────┬─────┐
    #   # │ a   ┆ b    ┆ c   │
    #   # │ --- ┆ ---  ┆ --- │
    #   # │ i64 ┆ f64  ┆ i64 │
    #   # ╞═════╪══════╪═════╡
    #   # │ 6   ┆ 20.0 ┆ 0   │
    #   # └─────┴──────┴─────┘
    def product
      select(Polars.all.product)
    end

    # Aggregate the columns of this DataFrame to their quantile value.
    #
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.quantile(0.5, interpolation: "nearest")
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 2.0 ┆ 7.0 ┆ null │
    #   # └─────┴─────┴──────┘
    def quantile(quantile, interpolation: "nearest")
      _from_rbdf(_df.quantile(quantile, interpolation))
    end

    # Get one hot encoded dummy variables.
    #
    # @param columns
    #   A subset of columns to convert to dummy variables. `nil` means
    #   "all columns".
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2],
    #       "bar" => [3, 4],
    #       "ham" => ["a", "b"]
    #     }
    #   )
    #   df.to_dummies
    #   # =>
    #   # shape: (2, 6)
    #   # ┌───────┬───────┬───────┬───────┬───────┬───────┐
    #   # │ foo_1 ┆ foo_2 ┆ bar_3 ┆ bar_4 ┆ ham_a ┆ ham_b │
    #   # │ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---   │
    #   # │ u8    ┆ u8    ┆ u8    ┆ u8    ┆ u8    ┆ u8    │
    #   # ╞═══════╪═══════╪═══════╪═══════╪═══════╪═══════╡
    #   # │ 1     ┆ 0     ┆ 1     ┆ 0     ┆ 1     ┆ 0     │
    #   # │ 0     ┆ 1     ┆ 0     ┆ 1     ┆ 0     ┆ 1     │
    #   # └───────┴───────┴───────┴───────┴───────┴───────┘
    def to_dummies(columns: nil, separator: "_", drop_first: false)
      if columns.is_a?(String)
        columns = [columns]
      end
      _from_rbdf(_df.to_dummies(columns, separator, drop_first))
    end

    # Drop duplicate rows from this DataFrame.
    #
    # @param maintain_order [Boolean]
    #   Keep the same order as the original DataFrame. This requires more work to
    #   compute.
    # @param subset [Object]
    #   Subset to use to compare rows.
    # @param keep ["first", "last"]
    #   Which of the duplicate rows to keep (in conjunction with `subset`).
    #
    # @return [DataFrame]
    #
    # @note
    #   Note that this fails if there is a column of type `List` in the DataFrame or
    #   subset.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 1, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 1.0, 2.0, 3.0, 3.0],
    #       "c" => [true, true, true, false, true, true]
    #     }
    #   )
    #   df.unique
    #   # =>
    #   # shape: (5, 3)
    #   # ┌─────┬─────┬───────┐
    #   # │ a   ┆ b   ┆ c     │
    #   # │ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ f64 ┆ bool  │
    #   # ╞═════╪═════╪═══════╡
    #   # │ 1   ┆ 0.5 ┆ true  │
    #   # │ 2   ┆ 1.0 ┆ true  │
    #   # │ 3   ┆ 2.0 ┆ false │
    #   # │ 4   ┆ 3.0 ┆ true  │
    #   # │ 5   ┆ 3.0 ┆ true  │
    #   # └─────┴─────┴───────┘
    def unique(maintain_order: true, subset: nil, keep: "first")
      self._from_rbdf(
        lazy
          .unique(maintain_order: maintain_order, subset: subset, keep: keep)
          .collect(no_optimization: true)
          ._df
      )
    end

    # Return the number of unique rows, or the number of unique row-subsets.
    #
    # @param subset [Object]
    #   One or more columns/expressions that define what to count;
    #   omit to return the count of unique rows.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 1, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 1.0, 2.0, 3.0, 3.0],
    #       "c" => [true, true, true, false, true, true]
    #     }
    #   )
    #   df.n_unique
    #   # => 5
    #
    # @example Simple columns subset
    #   df.n_unique(subset: ["b", "c"])
    #   # => 4
    #
    # @example Expression subset
    #   df.n_unique(
    #     subset: [
    #       (Polars.col("a").floordiv(2)),
    #       (Polars.col("c") | (Polars.col("b") >= 2))
    #     ]
    #   )
    #   # => 3
    def n_unique(subset: nil)
      if subset.is_a?(StringIO)
        subset = [Polars.col(subset)]
      elsif subset.is_a?(Expr)
        subset = [subset]
      end

      if subset.is_a?(::Array) && subset.length == 1
        expr = Utils.expr_to_lit_or_expr(subset[0], str_to_lit: false)
      else
        struct_fields = subset.nil? ? Polars.all : subset
        expr = Polars.struct(struct_fields)
      end

      df = lazy.select(expr.n_unique).collect
      df.is_empty ? 0 : df.row(0)[0]
    end

    # Rechunk the data in this DataFrame to a contiguous allocation.

    # This will make sure all subsequent operations have optimal and predictable
    # performance.
    #
    # @return [DataFrame]
    def rechunk
      _from_rbdf(_df.rechunk)
    end

    # Create a new DataFrame that shows the null counts per column.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, nil, 3],
    #       "bar" => [6, 7, nil],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.null_count
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ u32 ┆ u32 ┆ u32 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 1   ┆ 0   │
    #   # └─────┴─────┴─────┘
    def null_count
      _from_rbdf(_df.null_count)
    end

    # Sample from this DataFrame.
    #
    # @param n [Integer]
    #   Number of items to return. Cannot be used with `frac`. Defaults to 1 if
    #   `frac` is nil.
    # @param frac [Float]
    #   Fraction of items to return. Cannot be used with `n`.
    # @param with_replacement [Boolean]
    #   Allow values to be sampled more than once.
    # @param shuffle [Boolean]
    #   Shuffle the order of sampled data points.
    # @param seed [Integer]
    #   Seed for the random number generator. If set to nil (default), a random
    #   seed is used.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.sample(n: 2, seed: 0)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8   ┆ c   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # └─────┴─────┴─────┘
    def sample(
      n: nil,
      frac: nil,
      with_replacement: false,
      shuffle: false,
      seed: nil
    )
      if !n.nil? && !frac.nil?
        raise ArgumentError, "cannot specify both `n` and `frac`"
      end

      if n.nil? && !frac.nil?
        frac = Series.new("frac", [frac]) unless frac.is_a?(Series)

        _from_rbdf(
          _df.sample_frac(frac._s, with_replacement, shuffle, seed)
        )
      end

      if n.nil?
        n = 1
      end

      n = Series.new("", [n]) unless n.is_a?(Series)

      _from_rbdf(_df.sample_n(n._s, with_replacement, shuffle, seed))
    end

    # Apply a horizontal reduction on a DataFrame.
    #
    # This can be used to effectively determine aggregations on a row level, and can
    # be applied to any DataType that can be supercasted (casted to a similar parent
    # type).
    #
    # An example of the supercast rules when applying an arithmetic operation on two
    # DataTypes are for instance:
    #
    # i8 + str = str
    # f32 + i64 = f32
    # f32 + f64 = f64
    #
    # @return [Series]
    #
    # @example A horizontal sum operation:
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [2, 1, 3],
    #       "b" => [1, 2, 3],
    #       "c" => [1.0, 2.0, 3.0]
    #     }
    #   )
    #   df.fold { |s1, s2| s1 + s2 }
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         4.0
    #   #         5.0
    #   #         9.0
    #   # ]
    #
    # @example A horizontal minimum operation:
    #   df = Polars::DataFrame.new({"a" => [2, 1, 3], "b" => [1, 2, 3], "c" => [1.0, 2.0, 3.0]})
    #   df.fold { |s1, s2| s1.zip_with(s1 < s2, s2) }
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.0
    #   #         1.0
    #   #         3.0
    #   # ]
    #
    # @example A horizontal string concatenation:
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["foo", "bar", 2],
    #       "b" => [1, 2, 3],
    #       "c" => [1.0, 2.0, 3.0]
    #     }
    #   )
    #   df.fold { |s1, s2| s1 + s2 }
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [str]
    #   # [
    #   #         "foo11.0"
    #   #         "bar22.0"
    #   #         null
    #   # ]
    #
    # @example A horizontal boolean or, similar to a row-wise .any():
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [false, false, true],
    #       "b" => [false, true, false]
    #     }
    #   )
    #   df.fold { |s1, s2| s1 | s2 }
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         true
    #   # ]
    def fold(&operation)
      acc = to_series(0)

      1.upto(width - 1) do |i|
        acc = operation.call(acc, to_series(i))
      end
      acc
    end

    # Get a row as tuple, either by index or by predicate.
    #
    # @param index [Object]
    #   Row index.
    # @param by_predicate [Object]
    #   Select the row according to a given expression/predicate.
    # @param named [Boolean]
    #   Return a hash instead of an array. The hash is a mapping of
    #   column name to row value. This is more expensive than returning an
    #   array, but allows for accessing values by column name.
    #
    # @return [Object]
    #
    # @note
    #   The `index` and `by_predicate` params are mutually exclusive. Additionally,
    #   to ensure clarity, the `by_predicate` parameter must be supplied by keyword.
    #
    #   When using `by_predicate` it is an error condition if anything other than
    #   one row is returned; more than one row raises `TooManyRowsReturned`, and
    #   zero rows will raise `NoRowsReturned` (both inherit from `RowsException`).
    #
    # @example Return the row at the given index
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.row(2)
    #   # => [3, 8, "c"]
    #
    # @example Get a hash instead with a mapping of column names to row values
    #   df.row(2, named: true)
    #   # => {"foo"=>3, "bar"=>8, "ham"=>"c"}
    #
    # @example Return the row that matches the given predicate
    #   df.row(by_predicate: Polars.col("ham") == "b")
    #   # => [2, 7, "b"]
    def row(index = nil, by_predicate: nil, named: false)
      if !index.nil? && !by_predicate.nil?
        raise ArgumentError, "Cannot set both 'index' and 'by_predicate'; mutually exclusive"
      elsif index.is_a?(Expr)
        raise TypeError, "Expressions should be passed to the 'by_predicate' param"
      end

      if !index.nil?
        row = _df.row_tuple(index)
        if named
          columns.zip(row).to_h
        else
          row
        end
      elsif !by_predicate.nil?
        if !by_predicate.is_a?(Expr)
          raise TypeError, "Expected by_predicate to be an expression; found #{by_predicate.class.name}"
        end
        rows = filter(by_predicate).rows
        n_rows = rows.length
        if n_rows > 1
          raise TooManyRowsReturned, "Predicate #{by_predicate} returned #{n_rows} rows"
        elsif n_rows == 0
          raise NoRowsReturned, "Predicate #{by_predicate} returned no rows"
        end
        row = rows[0]
        if named
          columns.zip(row).to_h
        else
          row
        end
      else
        raise ArgumentError, "One of 'index' or 'by_predicate' must be set"
      end
    end

    # Convert columnar data to rows as Ruby arrays.
    #
    # @param named [Boolean]
    #   Return hashes instead of arrays. The hashes are a mapping of
    #   column name to row value. This is more expensive than returning an
    #   array, but allows for accessing values by column name.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.rows
    #   # => [[1, 2], [3, 4], [5, 6]]
    # @example
    #   df.rows(named: true)
    #   # => [{"a"=>1, "b"=>2}, {"a"=>3, "b"=>4}, {"a"=>5, "b"=>6}]
    def rows(named: false)
      if named
        columns = columns()
        _df.row_tuples.map do |v|
          columns.zip(v).to_h
        end
      else
        _df.row_tuples
      end
    end

    # Returns an iterator over the DataFrame of rows of Ruby-native values.
    #
    # @param named [Boolean]
    #   Return hashes instead of arrays. The hashes are a mapping of
    #   column name to row value. This is more expensive than returning an
    #   array, but allows for accessing values by column name.
    # @param buffer_size [Integer]
    #   Determines the number of rows that are buffered internally while iterating
    #   over the data; you should only modify this in very specific cases where the
    #   default value is determined not to be a good fit to your access pattern, as
    #   the speedup from using the buffer is significant (~2-4x). Setting this
    #   value to zero disables row buffering.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.iter_rows.map { |row| row[0] }
    #   # => [1, 3, 5]
    #
    # @example
    #   df.iter_rows(named: true).map { |row| row["b"] }
    #   # => [2, 4, 6]
    def iter_rows(named: false, buffer_size: 500, &block)
      return to_enum(:iter_rows, named: named, buffer_size: buffer_size) unless block_given?

      # load into the local namespace for a modest performance boost in the hot loops
      columns = columns()

      # note: buffering rows results in a 2-4x speedup over individual calls
      # to ".row(i)", so it should only be disabled in extremely specific cases.
      if buffer_size
        offset = 0
        while offset < height
          zerocopy_slice = slice(offset, buffer_size)
          rows_chunk = zerocopy_slice.rows(named: false)
          if named
            rows_chunk.each do |row|
              yield columns.zip(row).to_h
            end
          else
            rows_chunk.each(&block)
          end
          offset += buffer_size
        end
      elsif named
        height.times do |i|
          yield columns.zip(row(i)).to_h
        end
      else
        height.times do |i|
          yield row(i)
        end
      end
    end

    # Returns an iterator over the DataFrame of rows of Ruby-native values.
    #
    # @param named [Boolean]
    #   Return hashes instead of arrays. The hashes are a mapping of
    #   column name to row value. This is more expensive than returning an
    #   array, but allows for accessing values by column name.
    # @param buffer_size [Integer]
    #   Determines the number of rows that are buffered internally while iterating
    #   over the data; you should only modify this in very specific cases where the
    #   default value is determined not to be a good fit to your access pattern, as
    #   the speedup from using the buffer is significant (~2-4x). Setting this
    #   value to zero disables row buffering.
    #
    # @return [Object]
    def each_row(named: true, buffer_size: 500, &block)
      iter_rows(named: named, buffer_size: buffer_size, &block)
    end

    # Shrink DataFrame memory usage.
    #
    # Shrinks to fit the exact capacity needed to hold the data.
    #
    # @return [DataFrame]
    def shrink_to_fit(in_place: false)
      if in_place
        _df.shrink_to_fit
        self
      else
        df = clone
        df._df.shrink_to_fit
        df
      end
    end

    # Take every nth row in the DataFrame and return as a new DataFrame.
    #
    # @return [DataFrame]
    #
    # @example
    #   s = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [5, 6, 7, 8]})
    #   s.gather_every(2)
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
    def gather_every(n)
      select(Utils.col("*").gather_every(n))
    end
    alias_method :take_every, :gather_every

    # Hash and combine the rows in this DataFrame.
    #
    # The hash value is of type `:u64`.
    #
    # @param seed [Integer]
    #   Random seed parameter. Defaults to 0.
    # @param seed_1 [Integer]
    #   Random seed parameter. Defaults to `seed` if not set.
    # @param seed_2 [Integer]
    #   Random seed parameter. Defaults to `seed` if not set.
    # @param seed_3 [Integer]
    #   Random seed parameter. Defaults to `seed` if not set.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, nil, 3, 4],
    #       "ham" => ["a", "b", nil, "d"]
    #     }
    #   )
    #   df.hash_rows(seed: 42)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u64]
    #   # [
    #   #         4238614331852490969
    #   #         17976148875586754089
    #   #         4702262519505526977
    #   #         18144177983981041107
    #   # ]
    def hash_rows(seed: 0, seed_1: nil, seed_2: nil, seed_3: nil)
      k0 = seed
      k1 = seed_1.nil? ? seed : seed_1
      k2 = seed_2.nil? ? seed : seed_2
      k3 = seed_3.nil? ? seed : seed_3
      Utils.wrap_s(_df.hash_rows(k0, k1, k2, k3))
    end

    # Interpolate intermediate values. The interpolation method is linear.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, nil, 9, 10],
    #       "bar" => [6, 7, 9, nil],
    #       "baz" => [1, nil, nil, 9]
    #     }
    #   )
    #   df.interpolate
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

    # Check if the dataframe is empty.
    #
    # @return [Boolean]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   df.is_empty
    #   # => false
    #   df.filter(Polars.col("foo") > 99).is_empty
    #   # => true
    def is_empty
      height == 0
    end
    alias_method :empty?, :is_empty

    # Convert a `DataFrame` to a `Series` of type `Struct`.
    #
    # @param name [String]
    #   Name for the struct Series
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4, 5],
    #       "b" => ["one", "two", "three", "four", "five"]
    #     }
    #   )
    #   df.to_struct("nums")
    #   # =>
    #   # shape: (5,)
    #   # Series: 'nums' [struct[2]]
    #   # [
    #   #         {1,"one"}
    #   #         {2,"two"}
    #   #         {3,"three"}
    #   #         {4,"four"}
    #   #         {5,"five"}
    #   # ]
    def to_struct(name)
      Utils.wrap_s(_df.to_struct(name))
    end

    # Decompose a struct into its fields.
    #
    # The fields will be inserted into the `DataFrame` on the location of the
    # `struct` type.
    #
    # @param names [Object]
    #  Names of the struct columns that will be decomposed by its fields
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "before" => ["foo", "bar"],
    #       "t_a" => [1, 2],
    #       "t_b" => ["a", "b"],
    #       "t_c" => [true, nil],
    #       "t_d" => [[1, 2], [3]],
    #       "after" => ["baz", "womp"]
    #     }
    #   ).select(["before", Polars.struct(Polars.col("^t_.$")).alias("t_struct"), "after"])
    #   df.unnest("t_struct")
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
      _from_rbdf(_df.unnest(names))
    end

    # TODO
    # def corr
    # end

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
    # @return [DataFrame]
    def set_sorted(
      column,
      *more_columns,
      descending: false
    )
      lazy
        .set_sorted(column, *more_columns, descending: descending)
        .collect(no_optimization: true)
    end

    # TODO
    # def update
    # end

    private

    def initialize_copy(other)
      super
      self._df = _df._clone
    end

    def _pos_idx(idx, dim)
      if idx >= 0
        idx
      else
        shape[dim] + idx
      end
    end

    def _pos_idxs(idxs, dim)
      idx_type = Polars._get_idx_type

      if idxs.is_a?(Series)
        if idxs.dtype == idx_type
          return idxs
        end
        if [UInt8, UInt16, idx_type == UInt32 ? UInt64 : UInt32, Int8, Int16, Int32, Int64].include?(idxs.dtype)
          if idx_type == UInt32
            if [Int64, UInt64].include?(idxs.dtype)
              if idxs.max >= 2**32
                raise ArgumentError, "Index positions should be smaller than 2^32."
              end
            end
            if idxs.dtype == Int64
              if idxs.min < -(2**32)
                raise ArgumentError, "Index positions should be bigger than -2^32 + 1."
              end
            end
          end
          if [Int8, Int16, Int32, Int64].include?(idxs.dtype)
            if idxs.min < 0
              if idx_type == UInt32
                if [Int8, Int16].include?(idxs.dtype)
                  idxs = idxs.cast(Int32)
                end
              else
                if [Int8, Int16, Int32].include?(idxs.dtype)
                  idxs = idxs.cast(Int64)
                end
              end

              idxs =
                Polars.select(
                  Polars.when(Polars.lit(idxs) < 0)
                    .then(shape[dim] + Polars.lit(idxs))
                    .otherwise(Polars.lit(idxs))
                ).to_series
            end
          end

          return idxs.cast(idx_type)
        end
      end

      raise ArgumentError, "Unsupported idxs datatype."
    end

    # @private
    def self.expand_hash_scalars(data, schema_overrides: nil, order: nil, nan_to_null: false)
      updated_data = {}
      unless data.empty?
        dtypes = schema_overrides || {}
        array_len = data.values.map { |val| Utils.arrlen(val) || 0 }.max
        if array_len > 0
          data.each do |name, val|
            dtype = dtypes[name]
            if val.is_a?(Hash) && dtype != Struct
              updated_data[name] = DataFrame.new(val).to_struct(name)
            elsif !Utils.arrlen(val).nil?
              updated_data[name] = Series.new(String.new(name), val, dtype: dtype)
            elsif val.nil? || [Integer, Float, TrueClass, FalseClass, String, ::Date, ::DateTime, ::Time].any? { |cls| val.is_a?(cls) }
              dtype = Polars::Float64 if val.nil? && dtype.nil?
              updated_data[name] = Series.new(String.new(name), [val], dtype: dtype).extend_constant(val, array_len - 1)
            else
              raise Todo
            end
          end
        elsif data.values.all? { |val| Utils.arrlen(val) == 0 }
          data.each do |name, val|
            updated_data[name] = Series.new(name, val, dtype: dtypes[name])
          end
        elsif data.values.all? { |val| Utils.arrlen(val).nil? }
          data.each do |name, val|
            updated_data[name] = Series.new(name, [val], dtype: dtypes[name])
          end
        end
      end
      updated_data
    end

    # @private
    def self.hash_to_rbdf(data, schema: nil, schema_overrides: nil, nan_to_null: nil)
      if schema.is_a?(Hash) && !data.empty?
        if !data.all? { |col, _| schema[col] }
          raise ArgumentError, "The given column-schema names do not match the data dictionary"
        end

        data = schema.to_h { |col| [col, data[col]] }
      end

      column_names, schema_overrides = _unpack_schema(
        schema, lookup_names: data.keys, schema_overrides: schema_overrides
      )
      if column_names.empty?
        column_names = data.keys
      end

      if data.empty? && !schema_overrides.empty?
        data_series = column_names.map { |name| Series.new(name, [], dtype: schema_overrides[name], nan_to_null: nan_to_null)._s }
      else
        data_series = expand_hash_scalars(data, schema_overrides: schema_overrides, nan_to_null: nan_to_null).values.map(&:_s)
      end

      data_series = _handle_columns_arg(data_series, columns: column_names, from_hash: true)
      RbDataFrame.new(data_series)
    end

    # @private
    def self.include_unknowns(schema, cols)
      cols.to_h { |col| [col, schema.fetch(col, Unknown)] }
    end

    # @private
    def self._unpack_schema(schema, schema_overrides: nil, n_expected: nil, lookup_names: nil, include_overrides_in_columns: false)
      if schema.is_a?(Hash)
        schema = schema.to_a
      end
      column_names =
        (schema || []).map.with_index do |col, i|
          if col.is_a?(String)
            col || "column_#{i}"
          else
            col[0]
          end
        end
      if column_names.empty? && n_expected
        column_names = n_expected.times.map { |i| "column_#{i}" }
      end
      # TODO zip_longest
      lookup = column_names.zip(lookup_names || []).to_h

      column_dtypes =
        (schema || []).select { |col| !col.is_a?(String) && col[1] }.to_h do |col|
          [lookup[col[0]] || col[0], col[1]]
        end

      if schema_overrides && schema_overrides.any?
        column_dtypes.merge!(schema_overrides)
      end

      column_dtypes.each do |col, dtype|
        if !Utils.is_polars_dtype(dtype, include_unknown: true) && !dtype.nil?
          column_dtypes[col] = Utils.rb_type_to_dtype(dtype)
        end
      end

      [column_names, column_dtypes]
    end

    def self._handle_columns_arg(data, columns: nil, from_hash: false)
      if columns.nil? || columns.empty?
        data
      else
        if data.empty?
          columns.map { |c| Series.new(c, nil)._s }
        elsif data.length == columns.length
          if from_hash
            series_map = data.to_h { |s| [s.name, s] }
            if columns.all? { |col| series_map.key?(col) }
              return columns.map { |col| series_map[col] }
            end
          end

          columns.each_with_index do |c, i|
            # not in-place?
            data[i].rename(c)
          end
          data
        else
          raise ArgumentError, "Dimensions of columns arg must match data dimensions."
        end
      end
    end

    def self._post_apply_columns(rbdf, columns, structs: nil, schema_overrides: nil)
      rbdf_columns = rbdf.columns
      rbdf_dtypes = rbdf.dtypes
      columns, dtypes = _unpack_schema(
        (columns || rbdf_columns), schema_overrides: schema_overrides
      )
      column_subset = []
      if columns != rbdf_columns
        if columns.length < rbdf_columns.length && columns == rbdf_columns.first(columns.length)
          column_subset = columns
        else
          rbdf.set_column_names(columns)
        end
      end

      column_casts = []
      columns.each do |col, i|
        if dtypes[col] == Categorical # != rbdf_dtypes[i]
          column_casts << Polars.col(col).cast(Categorical)._rbexpr
        elsif structs&.any? && structs.include?(col) && structs[col] != rbdf_dtypes[i]
          column_casts << Polars.col(col).cast(structs[col])._rbexpr
        elsif dtypes.include?(col) && dtypes[col] != rbdf_dtypes[i]
          column_casts << Polars.col(col).cast(dtypes[col])._rbexpr
        end
      end

      if column_casts.any? || column_subset.any?
        rbdf = rbdf.lazy
        if column_casts.any?
          rbdf = rbdf.with_columns(column_casts)
        end
        if column_subset.any?
          rbdf = rbdf.select(column_subset.map { |col| Polars.col(col)._rbexpr })
        end
        rbdf = rbdf.collect
      end

      rbdf
    end

    # @private
    def self.sequence_to_rbdf(data, schema: nil, schema_overrides: nil, orient: nil, infer_schema_length: 50)
      raise Todo if schema_overrides
      columns = schema

      if data.length == 0
        return hash_to_rbdf({}, schema: schema, schema_overrides: schema_overrides)
      end

      if data[0].is_a?(Series)
        # series_names = data.map(&:name)
        # columns, dtypes = _unpack_schema(columns || series_names, n_expected: data.length)
        data_series = []
        data.each do |s|
          data_series << s._s
        end
      elsif data[0].is_a?(Hash)
        column_names, dtypes = _unpack_schema(columns)
        schema_overrides = dtypes ? include_unknowns(dtypes, column_names) : nil
        rbdf = RbDataFrame.read_hashes(data, infer_schema_length, schema_overrides)
        if column_names
          rbdf = _post_apply_columns(rbdf, column_names)
        end
        return rbdf
      elsif data[0].is_a?(::Array)
        if orient.nil? && !columns.nil?
          first_element = data[0]
          row_types = first_element.filter_map { |value| value.class }.uniq
          if row_types.include?(Integer) && row_types.include?(Float)
            row_types.delete(Integer)
          end
          orient = row_types.length == 1 ? "col" : "row"
        end

        if orient == "row"
          column_names, schema_overrides = _unpack_schema(
            schema, schema_overrides: schema_overrides, n_expected: first_element.length
          )
          local_schema_override = (
            schema_overrides.any? ? (raise Todo) : {}
          )
          if column_names.any? && first_element.length > 0 && first_element.length != column_names.length
            raise ArgumentError, "the row data does not match the number of columns"
          end

          unpack_nested = false
          local_schema_override.each do |col, tp|
            raise Todo
          end

          if unpack_nested
            raise Todo
          else
            rbdf = RbDataFrame.read_rows(
              data,
              infer_schema_length,
              local_schema_override.any? ? local_schema_override : nil
            )
          end
          if column_names.any? || schema_overrides.any?
            rbdf = _post_apply_columns(
              rbdf, column_names, schema_overrides: schema_overrides
            )
          end
          return rbdf
        elsif orient == "col" || orient.nil?
          column_names, schema_overrides = _unpack_schema(
            schema, schema_overrides: schema_overrides, n_expected: data.length
          )
          data_series =
            data.map.with_index do |element, i|
              Series.new(column_names[i], element, dtype: schema_overrides[column_names[i]])._s
            end
          return RbDataFrame.new(data_series)
        else
          raise ArgumentError, "orient must be one of {{'col', 'row', nil}}, got #{orient} instead."
        end
      end

      data_series = _handle_columns_arg(data_series, columns: columns)
      RbDataFrame.new(data_series)
    end

    # @private
    def self.series_to_rbdf(data, schema: nil, schema_overrides: nil)
      data_series = [data._s]
      series_name = data_series.map(&:name)
      column_names, schema_overrides = _unpack_schema(
        schema || series_name, schema_overrides: schema_overrides, n_expected: 1
      )
      if schema_overrides.any?
        new_dtype = schema_overrides.values[0]
        if new_dtype != data.dtype
          data_series[0] = data_series[0].cast(new_dtype, true)
        end
      end

      data_series = _handle_columns_arg(data_series, columns: column_names)
      RbDataFrame.new(data_series)
    end

    def wrap_ldf(ldf)
      LazyFrame._from_rbldf(ldf)
    end

    def _from_rbdf(rb_df)
      self.class._from_rbdf(rb_df)
    end

    def _comp(other, op)
      if other.is_a?(DataFrame)
        _compare_to_other_df(other, op)
      else
        _compare_to_non_df(other, op)
      end
    end

    def _compare_to_other_df(other, op)
      if columns != other.columns
        raise ArgumentError, "DataFrame columns do not match"
      end
      if shape != other.shape
        raise ArgumentError, "DataFrame dimensions do not match"
      end

      suffix = "__POLARS_CMP_OTHER"
      other_renamed = other.select(Polars.all.suffix(suffix))
      combined = Polars.concat([self, other_renamed], how: "horizontal")

      expr = case op
      when "eq"
        columns.map { |n| Polars.col(n) == Polars.col("#{n}#{suffix}") }
      when "neq"
        columns.map { |n| Polars.col(n) != Polars.col("#{n}#{suffix}") }
      when "gt"
        columns.map { |n| Polars.col(n) > Polars.col("#{n}#{suffix}") }
      when "lt"
        columns.map { |n| Polars.col(n) < Polars.col("#{n}#{suffix}") }
      when "gt_eq"
        columns.map { |n| Polars.col(n) >= Polars.col("#{n}#{suffix}") }
      when "lt_eq"
        columns.map { |n| Polars.col(n) <= Polars.col("#{n}#{suffix}") }
      else
        raise ArgumentError, "got unexpected comparison operator: #{op}"
      end

      combined.select(expr)
    end

    def _compare_to_non_df(other, op)
      case op
      when "eq"
        select(Polars.all == other)
      when "neq"
        select(Polars.all != other)
      when "gt"
        select(Polars.all > other)
      when "lt"
        select(Polars.all < other)
      when "gt_eq"
        select(Polars.all >= other)
      when "lt_eq"
        select(Polars.all <= other)
      else
        raise ArgumentError, "got unexpected comparison operator: #{op}"
      end
    end

    def _prepare_other_arg(other)
      if !other.is_a?(Series)
        if other.is_a?(::Array)
          raise ArgumentError, "Operation not supported."
        end

        other = Series.new("", [other])
      end
      other
    end
  end
end
