module Polars
  module Functions
    # Create polars `Duration` from distinct time components.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [DateTime.new(2022, 1, 1), DateTime.new(2022, 1, 2)],
    #       "add" => [1, 2]
    #     }
    #   )
    #   df.select(
    #     [
    #       (Polars.col("datetime") + Polars.duration(weeks: "add")).alias("add_weeks"),
    #       (Polars.col("datetime") + Polars.duration(days: "add")).alias("add_days"),
    #       (Polars.col("datetime") + Polars.duration(seconds: "add")).alias("add_seconds"),
    #       (Polars.col("datetime") + Polars.duration(milliseconds: "add")).alias(
    #         "add_milliseconds"
    #       ),
    #       (Polars.col("datetime") + Polars.duration(hours: "add")).alias("add_hours")
    #     ]
    #   )
    #   # =>
    #   # shape: (2, 5)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┬─────────────────────────┬─────────────────────┐
    #   # │ add_weeks           ┆ add_days            ┆ add_seconds         ┆ add_milliseconds        ┆ add_hours           │
    #   # │ ---                 ┆ ---                 ┆ ---                 ┆ ---                     ┆ ---                 │
    #   # │ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]            ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╪═════════════════════════╪═════════════════════╡
    #   # │ 2022-01-08 00:00:00 ┆ 2022-01-02 00:00:00 ┆ 2022-01-01 00:00:01 ┆ 2022-01-01 00:00:00.001 ┆ 2022-01-01 01:00:00 │
    #   # │ 2022-01-16 00:00:00 ┆ 2022-01-04 00:00:00 ┆ 2022-01-02 00:00:02 ┆ 2022-01-02 00:00:00.002 ┆ 2022-01-02 02:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┴─────────────────────────┴─────────────────────┘
    def duration(
      weeks: nil,
      days: nil,
      hours: nil,
      minutes: nil,
      seconds: nil,
      milliseconds: nil,
      microseconds: nil,
      nanoseconds: nil,
      time_unit: "us"
    )
      if !weeks.nil?
        weeks = Utils.expr_to_lit_or_expr(weeks, str_to_lit: false)._rbexpr
      end
      if !days.nil?
        days = Utils.expr_to_lit_or_expr(days, str_to_lit: false)._rbexpr
      end
      if !hours.nil?
        hours = Utils.expr_to_lit_or_expr(hours, str_to_lit: false)._rbexpr
      end
      if !minutes.nil?
        minutes = Utils.expr_to_lit_or_expr(minutes, str_to_lit: false)._rbexpr
      end
      if !seconds.nil?
        seconds = Utils.expr_to_lit_or_expr(seconds, str_to_lit: false)._rbexpr
      end
      if !milliseconds.nil?
        milliseconds = Utils.expr_to_lit_or_expr(milliseconds, str_to_lit: false)._rbexpr
      end
      if !microseconds.nil?
        microseconds = Utils.expr_to_lit_or_expr(microseconds, str_to_lit: false)._rbexpr
      end
      if !nanoseconds.nil?
        nanoseconds = Utils.expr_to_lit_or_expr(nanoseconds, str_to_lit: false)._rbexpr
      end

      Utils.wrap_expr(
        Plr.duration(
          weeks,
          days,
          hours,
          minutes,
          seconds,
          milliseconds,
          microseconds,
          nanoseconds,
          time_unit
        )
      )
    end

    # Concat the arrays in a Series dtype List in linear time.
    #
    # @return [Expr]
    def concat_list(exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(Plr.concat_list(exprs))
    end

    # Collect several columns into a Series of dtype Struct.
    #
    # @param exprs [Object]
    #   Columns/Expressions to collect into a Struct
    # @param eager [Boolean]
    #   Evaluate immediately
    #
    # @return [Object]
    #
    # @example
    #   Polars::DataFrame.new(
    #     {
    #       "int" => [1, 2],
    #       "str" => ["a", "b"],
    #       "bool" => [true, nil],
    #       "list" => [[1, 2], [3]],
    #     }
    #   ).select([Polars.struct(Polars.all).alias("my_struct")])
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────────┐
    #   # │ my_struct           │
    #   # │ ---                 │
    #   # │ struct[4]           │
    #   # ╞═════════════════════╡
    #   # │ {1,"a",true,[1, 2]} │
    #   # │ {2,"b",null,[3]}    │
    #   # └─────────────────────┘
    #
    # @example Only collect specific columns as a struct:
    #   df = Polars::DataFrame.new(
    #     {"a" => [1, 2, 3, 4], "b" => ["one", "two", "three", "four"], "c" => [9, 8, 7, 6]}
    #   )
    #   df.with_column(Polars.struct(Polars.col(["a", "b"])).alias("a_and_b"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────┬───────┬─────┬─────────────┐
    #   # │ a   ┆ b     ┆ c   ┆ a_and_b     │
    #   # │ --- ┆ ---   ┆ --- ┆ ---         │
    #   # │ i64 ┆ str   ┆ i64 ┆ struct[2]   │
    #   # ╞═════╪═══════╪═════╪═════════════╡
    #   # │ 1   ┆ one   ┆ 9   ┆ {1,"one"}   │
    #   # │ 2   ┆ two   ┆ 8   ┆ {2,"two"}   │
    #   # │ 3   ┆ three ┆ 7   ┆ {3,"three"} │
    #   # │ 4   ┆ four  ┆ 6   ┆ {4,"four"}  │
    #   # └─────┴───────┴─────┴─────────────┘
    def struct(exprs, eager: false)
      if eager
        Polars.select(struct(exprs, eager: false)).to_series
      end
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(Plr.as_struct(exprs))
    end

    # Horizontally concat Utf8 Series in linear time. Non-Utf8 columns are cast to Utf8.
    #
    # @param exprs [Object]
    #   Columns to concat into a Utf8 Series.
    # @param sep [String]
    #   String value that will be used to separate the values.
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["dogs", "cats", nil],
    #       "c" => ["play", "swim", "walk"]
    #     }
    #   )
    #   df.with_columns(
    #     [
    #       Polars.concat_str(
    #         [
    #           Polars.col("a") * 2,
    #           Polars.col("b"),
    #           Polars.col("c")
    #         ],
    #         sep: " "
    #       ).alias("full_sentence")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬──────┬───────────────┐
    #   # │ a   ┆ b    ┆ c    ┆ full_sentence │
    #   # │ --- ┆ ---  ┆ ---  ┆ ---           │
    #   # │ i64 ┆ str  ┆ str  ┆ str           │
    #   # ╞═════╪══════╪══════╪═══════════════╡
    #   # │ 1   ┆ dogs ┆ play ┆ 2 dogs play   │
    #   # │ 2   ┆ cats ┆ swim ┆ 4 cats swim   │
    #   # │ 3   ┆ null ┆ walk ┆ null          │
    #   # └─────┴──────┴──────┴───────────────┘
    def concat_str(exprs, sep: "", ignore_nulls: false)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(Plr.concat_str(exprs, sep, ignore_nulls))
    end

    # Format expressions as a string.
    #
    # @param fstring [String]
    #   A string that with placeholders.
    #   For example: "hello_{}" or "{}_world
    # @param args [Object]
    #   Expression(s) that fill the placeholders
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a": ["a", "b", "c"],
    #       "b": [1, 2, 3]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.format("foo_{}_bar_{}", Polars.col("a"), "b").alias("fmt")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────┐
    #   # │ fmt         │
    #   # │ ---         │
    #   # │ str         │
    #   # ╞═════════════╡
    #   # │ foo_a_bar_1 │
    #   # │ foo_b_bar_2 │
    #   # │ foo_c_bar_3 │
    #   # └─────────────┘
    def format(fstring, *args)
      if fstring.scan("{}").length != args.length
        raise ArgumentError, "number of placeholders should equal the number of arguments"
      end

      exprs = []

      arguments = args.each
      fstring.split(/(\{\})/).each do |s|
        if s == "{}"
          e = Utils.expr_to_lit_or_expr(arguments.next, str_to_lit: false)
          exprs << e
        elsif s.length > 0
          exprs << lit(s)
        end
      end

      concat_str(exprs, sep: "")
    end
  end
end
