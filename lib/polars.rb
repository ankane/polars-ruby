# ext
begin
  require "polars/#{RUBY_VERSION.to_f}/polars"
rescue LoadError
  require "polars/polars"
end

# stdlib
require "date"

# modules
require "polars/expr_dispatch"
require "polars/batched_csv_reader"
require "polars/cat_expr"
require "polars/cat_name_space"
require "polars/convert"
require "polars/data_frame"
require "polars/date_time_expr"
require "polars/date_time_name_space"
require "polars/dynamic_group_by"
require "polars/exceptions"
require "polars/expr"
require "polars/functions"
require "polars/group_by"
require "polars/io"
require "polars/lazy_frame"
require "polars/lazy_functions"
require "polars/lazy_group_by"
require "polars/list_expr"
require "polars/list_name_space"
require "polars/meta_expr"
require "polars/rolling_group_by"
require "polars/series"
require "polars/slice"
require "polars/string_expr"
require "polars/string_name_space"
require "polars/struct_expr"
require "polars/struct_name_space"
require "polars/utils"
require "polars/version"
require "polars/when"
require "polars/when_then"

module Polars
  extend Convert
  extend Functions
  extend IO
  extend LazyFunctions
end
