module Polars
  module IO
    # Read a CSV file into a DataFrame.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param has_header [Boolean]
    #   Indicate if the first row of dataset is a header or not.
    #   If set to false, column names will be autogenerated in the
    #   following format: `column_x`, with `x` being an
    #   enumeration over every column in the dataset starting at 1.
    # @param columns [Object]
    #   Columns to select. Accepts a list of column indices (starting
    #   at zero) or a list of column names.
    # @param new_columns [Object]
    #   Rename columns right after parsing the CSV file. If the given
    #   list is shorter than the width of the DataFrame the remaining
    #   columns will have their original name.
    # @param sep [String]
    #   Single byte character to use as delimiter in the file.
    # @param comment_char [String]
    #   Single byte character that indicates the start of a comment line,
    #   for instance `#`.
    # @param quote_char [String]
    #   Single byte character used for csv quoting.
    #   Set to nil to turn off special handling and escaping of quotes.
    # @param skip_rows [Integer]
    #   Start reading after `skip_rows` lines.
    # @param dtypes [Object]
    #   Overwrite dtypes during inference.
    # @param null_values [Object]
    #   Values to interpret as null values. You can provide a:
    #
    #   - `String`: All values equal to this string will be null.
    #   - `Array`: All values equal to any string in this array will be null.
    #   - `Hash`: A hash that maps column name to a null value string.
    # @param ignore_errors [Boolean]
    #   Try to keep reading lines if some lines yield errors.
    #   First try `infer_schema_length: 0` to read all columns as
    #   `:str` to check which values might cause an issue.
    # @param parse_dates [Boolean]
    #   Try to automatically parse dates. If this does not succeed,
    #   the column remains of data type `:str`.
    # @param n_threads [Integer]
    #   Number of threads to use in csv parsing.
    #   Defaults to the number of physical cpu's of your system.
    # @param infer_schema_length [Integer]
    #   Maximum number of lines to read to infer schema.
    #   If set to 0, all columns will be read as `:utf8`.
    #   If set to `nil`, a full table scan will be done (slow).
    # @param batch_size [Integer]
    #   Number of lines to read into the buffer at once.
    #   Modify this to change performance.
    # @param n_rows [Integer]
    #   Stop reading from CSV file after reading `n_rows`.
    #   During multi-threaded parsing, an upper bound of `n_rows`
    #   rows cannot be guaranteed.
    # @param encoding ["utf8", "utf8-lossy"]
    #   Lossy means that invalid utf8 values are replaced with `�`
    #   characters. When using other encodings than `utf8` or
    #   `utf8-lossy`, the input is first decoded im memory with
    #   Ruby.
    # @param low_memory [Boolean]
    #   Reduce memory usage at expense of performance.
    # @param rechunk [Boolean]
    #   Make sure that all columns are contiguous in memory by
    #   aggregating the chunks into a single array.
    # @param storage_options [Hash]
    #   Extra options that make sense for a
    #   particular storage connection.
    # @param skip_rows_after_header [Integer]
    #   Skip this number of rows when the header is parsed.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with the given name into
    #   the DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only used if the name is set).
    # @param sample_size [Integer]
    #   Set the sample size. This is used to sample statistics to estimate the
    #   allocation needed.
    # @param eol_char [String]
    #   Single byte end of line character.
    # @param truncate_ragged_lines [Boolean]
    #   Truncate lines that are longer than the schema.
    #
    # @return [DataFrame]
    #
    # @note
    #   This operation defaults to a `rechunk` operation at the end, meaning that
    #   all data will be stored continuously in memory.
    #   Set `rechunk: false` if you are benchmarking the csv-reader. A `rechunk` is
    #   an expensive operation.
    def read_csv(
      source,
      has_header: true,
      columns: nil,
      new_columns: nil,
      sep: ",",
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
      storage_options: nil,
      skip_rows_after_header: 0,
      row_count_name: nil,
      row_count_offset: 0,
      sample_size: 1024,
      eol_char: "\n",
      truncate_ragged_lines: false
    )
      Utils._check_arg_is_1byte("sep", sep, false)
      Utils._check_arg_is_1byte("comment_char", comment_char, false)
      Utils._check_arg_is_1byte("quote_char", quote_char, true)
      Utils._check_arg_is_1byte("eol_char", eol_char, false)

      projection, columns = Utils.handle_projection_columns(columns)

      storage_options ||= {}

      if columns && !has_header
        columns.each do |column|
          if !column.start_with?("column_")
            raise ArgumentError, "Specified column names do not start with \"column_\", but autogenerated header names were requested."
          end
        end
      end

      if projection || new_columns
        raise Todo
      end

      df = nil
      _prepare_file_arg(source) do |data|
        df = DataFrame._read_csv(
          data,
          has_header: has_header,
          columns: columns || projection,
          sep: sep,
          comment_char: comment_char,
          quote_char: quote_char,
          skip_rows: skip_rows,
          dtypes: dtypes,
          null_values: null_values,
          ignore_errors: ignore_errors,
          parse_dates: parse_dates,
          n_threads: n_threads,
          infer_schema_length: infer_schema_length,
          batch_size: batch_size,
          n_rows: n_rows,
          encoding: encoding == "utf8-lossy" ? encoding : "utf8",
          low_memory: low_memory,
          rechunk: rechunk,
          skip_rows_after_header: skip_rows_after_header,
          row_count_name: row_count_name,
          row_count_offset: row_count_offset,
          sample_size: sample_size,
          eol_char: eol_char,
          truncate_ragged_lines: truncate_ragged_lines
        )
      end

      if new_columns
        Utils._update_columns(df, new_columns)
      else
        df
      end
    end

    # Lazily read from a CSV file or multiple files via glob patterns.
    #
    # This allows the query optimizer to push down predicates and
    # projections to the scan level, thereby potentially reducing
    # memory overhead.
    #
    # @param source [Object]
    #   Path to a file.
    # @param has_header [Boolean]
    #   Indicate if the first row of dataset is a header or not.
    #   If set to false, column names will be autogenerated in the
    #   following format: `column_x`, with `x` being an
    #   enumeration over every column in the dataset starting at 1.
    # @param sep [String]
    #   Single byte character to use as delimiter in the file.
    # @param comment_char [String]
    #   Single byte character that indicates the start of a comment line,
    #   for instance `#`.
    # @param quote_char [String]
    #   Single byte character used for csv quoting.
    #   Set to None to turn off special handling and escaping of quotes.
    # @param skip_rows [Integer]
    #   Start reading after `skip_rows` lines. The header will be parsed at this
    #   offset.
    # @param dtypes [Object]
    #   Overwrite dtypes during inference.
    # @param null_values [Object]
    #   Values to interpret as null values. You can provide a:
    #
    #   - `String`: All values equal to this string will be null.
    #   - `Array`: All values equal to any string in this array will be null.
    #   - `Hash`: A hash that maps column name to a null value string.
    # @param ignore_errors [Boolean]
    #   Try to keep reading lines if some lines yield errors.
    #   First try `infer_schema_length: 0` to read all columns as
    #   `:str` to check which values might cause an issue.
    # @param cache [Boolean]
    #   Cache the result after reading.
    # @param with_column_names [Object]
    #   Apply a function over the column names.
    #   This can be used to update a schema just in time, thus before
    #   scanning.
    # @param infer_schema_length [Integer]
    #   Maximum number of lines to read to infer schema.
    #   If set to 0, all columns will be read as `:str`.
    #   If set to `nil`, a full table scan will be done (slow).
    # @param n_rows [Integer]
    #   Stop reading from CSV file after reading `n_rows`.
    # @param encoding ["utf8", "utf8-lossy"]
    #   Lossy means that invalid utf8 values are replaced with `�`
    #   characters.
    # @param low_memory [Boolean]
    #   Reduce memory usage in expense of performance.
    # @param rechunk [Boolean]
    #   Reallocate to contiguous memory when all chunks/ files are parsed.
    # @param skip_rows_after_header [Integer]
    #   Skip this number of rows when the header is parsed.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with the given name into
    #   the DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only used if the name is set).
    # @param parse_dates [Boolean]
    #   Try to automatically parse dates. If this does not succeed,
    #   the column remains of data type `:str`.
    # @param eol_char [String]
    #   Single byte end of line character.
    # @param truncate_ragged_lines [Boolean]
    #   Truncate lines that are longer than the schema.
    #
    # @return [LazyFrame]
    def scan_csv(
      source,
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
      eol_char: "\n",
      truncate_ragged_lines: false
    )
      Utils._check_arg_is_1byte("sep", sep, false)
      Utils._check_arg_is_1byte("comment_char", comment_char, false)
      Utils._check_arg_is_1byte("quote_char", quote_char, true)

      if Utils.pathlike?(source)
        source = Utils.normalise_filepath(source)
      end

      LazyFrame._scan_csv(
        source,
        has_header: has_header,
        sep: sep,
        comment_char: comment_char,
        quote_char: quote_char,
        skip_rows: skip_rows,
        dtypes: dtypes,
        null_values: null_values,
        ignore_errors: ignore_errors,
        cache: cache,
        with_column_names: with_column_names,
        infer_schema_length: infer_schema_length,
        n_rows: n_rows,
        low_memory: low_memory,
        rechunk: rechunk,
        skip_rows_after_header: skip_rows_after_header,
        encoding: encoding,
        row_count_name: row_count_name,
        row_count_offset: row_count_offset,
        parse_dates: parse_dates,
        eol_char: eol_char,
        truncate_ragged_lines: truncate_ragged_lines
      )
    end

    # Lazily read from an Arrow IPC (Feather v2) file or multiple files via glob patterns.
    #
    # This allows the query optimizer to push down predicates and projections to the scan
    # level, thereby potentially reducing memory overhead.
    #
    # @param source [String]
    #   Path to a IPC file.
    # @param n_rows [Integer]
    #   Stop reading from IPC file after reading `n_rows`.
    # @param cache [Boolean]
    #   Cache the result after reading.
    # @param rechunk [Boolean]
    #   Reallocate to contiguous memory when all chunks/ files are parsed.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param memory_map [Boolean]
    #   Try to memory map the file. This can greatly improve performance on repeated
    #   queries as the OS may cache pages.
    #   Only uncompressed IPC files can be memory mapped.
    #
    # @return [LazyFrame]
    def scan_ipc(
      source,
      n_rows: nil,
      cache: true,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0,
      storage_options: nil,
      memory_map: true
    )
      LazyFrame._scan_ipc(
        source,
        n_rows: n_rows,
        cache: cache,
        rechunk: rechunk,
        row_count_name: row_count_name,
        row_count_offset: row_count_offset,
        storage_options: storage_options,
        memory_map: memory_map
      )
    end

    # Lazily read from a parquet file or multiple files via glob patterns.
    #
    # This allows the query optimizer to push down predicates and projections to the scan
    # level, thereby potentially reducing memory overhead.
    #
    # @param source [String]
    #   Path to a file.
    # @param n_rows [Integer]
    #   Stop reading from parquet file after reading `n_rows`.
    # @param cache [Boolean]
    #   Cache the result after reading.
    # @param parallel ["auto", "columns", "row_groups", "none"]
    #   This determines the direction of parallelism. 'auto' will try to determine the
    #   optimal direction.
    # @param rechunk [Boolean]
    #   In case of reading multiple files via a glob pattern rechunk the final DataFrame
    #   into contiguous memory chunks.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param low_memory [Boolean]
    #   Reduce memory pressure at the expense of performance.
    #
    # @return [LazyFrame]
    def scan_parquet(
      source,
      n_rows: nil,
      cache: true,
      parallel: "auto",
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0,
      storage_options: nil,
      low_memory: false
    )
      if Utils.pathlike?(source)
        source = Utils.normalise_filepath(source)
      end

      LazyFrame._scan_parquet(
        source,
        n_rows:n_rows,
        cache: cache,
        parallel: parallel,
        rechunk: rechunk,
        row_count_name: row_count_name,
        row_count_offset: row_count_offset,
        storage_options: storage_options,
        low_memory: low_memory
      )
    end

    # Lazily read from a newline delimited JSON file.
    #
    # This allows the query optimizer to push down predicates and projections to the scan
    # level, thereby potentially reducing memory overhead.
    #
    # @param source [String]
    #   Path to a file.
    # @param infer_schema_length [Integer]
    #   Infer the schema length from the first `infer_schema_length` rows.
    # @param batch_size [Integer]
    #   Number of rows to read in each batch.
    # @param n_rows [Integer]
    #   Stop reading from JSON file after reading `n_rows`.
    # @param low_memory [Boolean]
    #   Reduce memory pressure at the expense of performance.
    # @param rechunk [Boolean]
    #   Reallocate to contiguous memory when all chunks/ files are parsed.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    #
    # @return [LazyFrame]
    def scan_ndjson(
      source,
      infer_schema_length: 100,
      batch_size: 1024,
      n_rows: nil,
      low_memory: false,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0
    )
      if Utils.pathlike?(source)
        source = Utils.normalise_filepath(source)
      end

      LazyFrame._scan_ndjson(
        source,
        infer_schema_length: infer_schema_length,
        batch_size: batch_size,
        n_rows: n_rows,
        low_memory: low_memory,
        rechunk: rechunk,
        row_count_name: row_count_name,
        row_count_offset: row_count_offset,
      )
    end

    # Read into a DataFrame from Apache Avro format.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param columns [Object]
    #   Columns to select. Accepts a list of column indices (starting at zero) or a list
    #   of column names.
    # @param n_rows [Integer]
    #   Stop reading from Apache Avro file after reading ``n_rows``.
    #
    # @return [DataFrame]
    def read_avro(source, columns: nil, n_rows: nil)
      if Utils.pathlike?(source)
        source = Utils.normalise_filepath(source)
      end

      DataFrame._read_avro(source, n_rows: n_rows, columns: columns)
    end

    # Read into a DataFrame from Arrow IPC (Feather v2) file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param columns [Object]
    #   Columns to select. Accepts a list of column indices (starting at zero) or a list
    #   of column names.
    # @param n_rows [Integer]
    #   Stop reading from IPC file after reading `n_rows`.
    # @param memory_map [Boolean]
    #   Try to memory map the file. This can greatly improve performance on repeated
    #   queries as the OS may cache pages.
    #   Only uncompressed IPC files can be memory mapped.
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param rechunk [Boolean]
    #   Make sure that all data is contiguous.
    #
    # @return [DataFrame]
    def read_ipc(
      source,
      columns: nil,
      n_rows: nil,
      memory_map: true,
      storage_options: nil,
      row_count_name: nil,
      row_count_offset: 0,
      rechunk: true
    )
      storage_options ||= {}
      _prepare_file_arg(source, **storage_options) do |data|
        DataFrame._read_ipc(
          data,
          columns: columns,
          n_rows: n_rows,
          row_count_name: row_count_name,
          row_count_offset: row_count_offset,
          rechunk: rechunk,
          memory_map: memory_map
        )
      end
    end

    # Read into a DataFrame from a parquet file.
    #
    # @param source [String, Pathname, StringIO]
    #   Path to a file or a file-like object.
    # @param columns [Object]
    #   Columns to select. Accepts a list of column indices (starting at zero) or a list
    #   of column names.
    # @param n_rows [Integer]
    #   Stop reading from parquet file after reading `n_rows`.
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param parallel ["auto", "columns", "row_groups", "none"]
    #   This determines the direction of parallelism. 'auto' will try to determine the
    #   optimal direction.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param low_memory [Boolean]
    #   Reduce memory pressure at the expense of performance.
    # @param use_statistics [Boolean]
    #   Use statistics in the parquet to determine if pages
    #   can be skipped from reading.
    # @param rechunk [Boolean]
    #   Make sure that all columns are contiguous in memory by
    #   aggregating the chunks into a single array.
    #
    # @return [DataFrame]
    #
    # @note
    #   This operation defaults to a `rechunk` operation at the end, meaning that
    #   all data will be stored continuously in memory.
    #   Set `rechunk: false` if you are benchmarking the parquet-reader. A `rechunk` is
    #   an expensive operation.
    def read_parquet(
      source,
      columns: nil,
      n_rows: nil,
      storage_options: nil,
      parallel: "auto",
      row_count_name: nil,
      row_count_offset: 0,
      low_memory: false,
      use_statistics: true,
      rechunk: true
    )
      _prepare_file_arg(source) do |data|
        DataFrame._read_parquet(
          data,
          columns: columns,
          n_rows: n_rows,
          parallel: parallel,
          row_count_name: row_count_name,
          row_count_offset: row_count_offset,
          low_memory: low_memory,
          use_statistics: use_statistics,
          rechunk: rechunk
        )
      end
    end

    # Read into a DataFrame from a JSON file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [DataFrame]
    def read_json(source)
      DataFrame._read_json(source)
    end

    # Read into a DataFrame from a newline delimited JSON file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [DataFrame]
    def read_ndjson(source)
      DataFrame._read_ndjson(source)
    end

    # Read a SQL query into a DataFrame.
    #
    # @param query [Object]
    #   ActiveRecord::Relation or ActiveRecord::Result.
    # @param schema_overrides [Hash]
    #   A hash mapping column names to dtypes, used to override the schema
    #   inferred from the query.
    #
    # @return [DataFrame]
    def read_database(query, schema_overrides: nil)
      if !defined?(ActiveRecord)
        raise Error, "Active Record not available"
      end

      result =
        if query.is_a?(ActiveRecord::Result)
          query
        elsif query.is_a?(ActiveRecord::Relation)
          query.connection.select_all(query.to_sql)
        elsif query.is_a?(::String)
          ActiveRecord::Base.connection.select_all(query)
        else
          raise ArgumentError, "Expected ActiveRecord::Relation, ActiveRecord::Result, or String"
        end

      data = {}
      schema_overrides = (schema_overrides || {}).transform_keys(&:to_s)

      result.columns.each_with_index do |k, i|
        column_type = result.column_types[i]

        data[k] =
          if column_type
            result.rows.map { |r| column_type.deserialize(r[i]) }
          else
            result.rows.map { |r| r[i] }
          end

        polars_type =
          case column_type&.type
          when :binary
            Binary
          when :boolean
            Boolean
          when :date
            Date
          when :datetime, :timestamp
            Datetime
          when :decimal
            Decimal
          when :float
            Float64
          when :integer
            Int64
          when :string, :text
            String
          when :time
            Time
          # TODO fix issue with null
          # when :json, :jsonb
          #   Struct
          end

        schema_overrides[k] ||= polars_type if polars_type
      end

      DataFrame.new(data, schema_overrides: schema_overrides)
    end
    alias_method :read_sql, :read_database

    # def read_excel
    # end

    # Read a CSV file in batches.
    #
    # Upon creation of the `BatchedCsvReader`,
    # polars will gather statistics and determine the
    # file chunks. After that work will only be done
    # if `next_batches` is called.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param has_header [Boolean]
    #   Indicate if the first row of dataset is a header or not.
    #   If set to False, column names will be autogenerated in the
    #   following format: `column_x`, with `x` being an
    #   enumeration over every column in the dataset starting at 1.
    # @param columns [Object]
    #   Columns to select. Accepts a list of column indices (starting
    #   at zero) or a list of column names.
    # @param new_columns [Object]
    #   Rename columns right after parsing the CSV file. If the given
    #   list is shorter than the width of the DataFrame the remaining
    #   columns will have their original name.
    # @param sep [String]
    #   Single byte character to use as delimiter in the file.
    # @param comment_char [String]
    #   Single byte character that indicates the start of a comment line,
    #   for instance `#`.
    # @param quote_char [String]
    #   Single byte character used for csv quoting, default = `"`.
    #   Set to nil to turn off special handling and escaping of quotes.
    # @param skip_rows [Integer]
    #   Start reading after `skip_rows` lines.
    # @param dtypes [Object]
    #   Overwrite dtypes during inference.
    # @param null_values [Object]
    #   Values to interpret as null values. You can provide a:
    #
    #   - `String`: All values equal to this string will be null.
    #   - `Array`: All values equal to any string in this array will be null.
    #   - `Hash`: A hash that maps column name to a null value string.
    # @param ignore_errors [Boolean]
    #   Try to keep reading lines if some lines yield errors.
    #   First try `infer_schema_length: 0` to read all columns as
    #   `:str` to check which values might cause an issue.
    # @param parse_dates [Boolean]
    #   Try to automatically parse dates. If this does not succeed,
    #   the column remains of data type `:str`.
    # @param n_threads [Integer]
    #   Number of threads to use in csv parsing.
    #   Defaults to the number of physical cpu's of your system.
    # @param infer_schema_length [Integer]
    #   Maximum number of lines to read to infer schema.
    #   If set to 0, all columns will be read as `:str`.
    #   If set to `nil`, a full table scan will be done (slow).
    # @param batch_size [Integer]
    #   Number of lines to read into the buffer at once.
    #   Modify this to change performance.
    # @param n_rows [Integer]
    #   Stop reading from CSV file after reading `n_rows`.
    #   During multi-threaded parsing, an upper bound of `n_rows`
    #   rows cannot be guaranteed.
    # @param encoding ["utf8", "utf8-lossy"]
    #   Lossy means that invalid utf8 values are replaced with `�`
    #   characters. When using other encodings than `utf8` or
    #   `utf8-lossy`, the input is first decoded im memory with
    #   Ruby. Defaults to `utf8`.
    # @param low_memory [Boolean]
    #   Reduce memory usage at expense of performance.
    # @param rechunk [Boolean]
    #   Make sure that all columns are contiguous in memory by
    #   aggregating the chunks into a single array.
    # @param skip_rows_after_header [Integer]
    #   Skip this number of rows when the header is parsed.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with the given name into
    #   the DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only used if the name is set).
    # @param sample_size [Integer]
    #   Set the sample size. This is used to sample statistics to estimate the
    #   allocation needed.
    # @param eol_char [String]
    #   Single byte end of line character.
    # @param truncate_ragged_lines [Boolean]
    #   Truncate lines that are longer than the schema.
    #
    # @return [BatchedCsvReader]
    #
    # @example
    #   reader = Polars.read_csv_batched(
    #     "./tpch/tables_scale_100/lineitem.tbl", sep: "|", parse_dates: true
    #   )
    #   reader.next_batches(5)
    def read_csv_batched(
      source,
      has_header: true,
      columns: nil,
      new_columns: nil,
      sep: ",",
      comment_char: nil,
      quote_char: '"',
      skip_rows: 0,
      dtypes: nil,
      null_values: nil,
      ignore_errors: false,
      parse_dates: false,
      n_threads: nil,
      infer_schema_length: 100,
      batch_size: 50_000,
      n_rows: nil,
      encoding: "utf8",
      low_memory: false,
      rechunk: true,
      skip_rows_after_header: 0,
      row_count_name: nil,
      row_count_offset: 0,
      sample_size: 1024,
      eol_char: "\n",
      truncate_ragged_lines: false
    )
      projection, columns = Utils.handle_projection_columns(columns)

      if columns && !has_header
        columns.each do |column|
          if !column.start_with?("column_")
            raise ArgumentError, "Specified column names do not start with \"column_\", but autogenerated header names were requested."
          end
        end
      end

      if projection || new_columns
        raise Todo
      end

      BatchedCsvReader.new(
        source,
        has_header: has_header,
        columns: columns || projection,
        sep: sep,
        comment_char: comment_char,
        quote_char: quote_char,
        skip_rows: skip_rows,
        dtypes: dtypes,
        null_values: null_values,
        ignore_errors: ignore_errors,
        parse_dates: parse_dates,
        n_threads: n_threads,
        infer_schema_length: infer_schema_length,
        batch_size: batch_size,
        n_rows: n_rows,
        encoding: encoding == "utf8-lossy" ? encoding : "utf8",
        low_memory: low_memory,
        rechunk: rechunk,
        skip_rows_after_header: skip_rows_after_header,
        row_count_name: row_count_name,
        row_count_offset: row_count_offset,
        sample_size: sample_size,
        eol_char: eol_char,
        new_columns: new_columns,
        truncate_ragged_lines: truncate_ragged_lines
      )
    end

    # Get a schema of the IPC file without reading data.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [Hash]
    def read_ipc_schema(source)
      if Utils.pathlike?(source)
        source = Utils.normalise_filepath(source)
      end

      Plr.ipc_schema(source)
    end

    # Get a schema of the Parquet file without reading data.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [Hash]
    def read_parquet_schema(source)
      if Utils.pathlike?(source)
        source = Utils.normalise_filepath(source)
      end

      Plr.parquet_schema(source)
    end

    private

    def _prepare_file_arg(file)
      if file.is_a?(::String) && file =~ /\Ahttps?:\/\//
        raise ArgumentError, "use URI(...) for remote files"
      end

      if defined?(URI) && file.is_a?(URI)
        require "open-uri"

        file = file.open
      end

      yield file
    end
  end
end
