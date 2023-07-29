use magnus::{value::Lazy, Module, RClass, RModule, Ruby};

static POLARS: Lazy<RModule> = Lazy::new(|ruby| ruby.class_object().const_get("Polars").unwrap());

pub(crate) fn polars() -> RModule {
    Ruby::get().unwrap().get_inner(&POLARS)
}

static SERIES: Lazy<RClass> =
    Lazy::new(|ruby| ruby.get_inner(&POLARS).const_get("Series").unwrap());

pub(crate) fn series() -> RClass {
    Ruby::get().unwrap().get_inner(&SERIES)
}

static UTILS: Lazy<RModule> = Lazy::new(|ruby| ruby.get_inner(&POLARS).const_get("Utils").unwrap());

pub(crate) fn utils() -> RModule {
    Ruby::get().unwrap().get_inner(&UTILS)
}

static BIGDECIMAL: Lazy<RClass> =
    Lazy::new(|ruby| ruby.class_object().const_get("BigDecimal").unwrap());

pub(crate) fn bigdecimal() -> RClass {
    Ruby::get().unwrap().get_inner(&BIGDECIMAL)
}

static DATE: Lazy<RClass> = Lazy::new(|ruby| ruby.class_object().const_get("Date").unwrap());

pub(crate) fn date() -> RClass {
    Ruby::get().unwrap().get_inner(&DATE)
}

static DATETIME: Lazy<RClass> =
    Lazy::new(|ruby| ruby.class_object().const_get("DateTime").unwrap());

pub(crate) fn datetime() -> RClass {
    Ruby::get().unwrap().get_inner(&DATETIME)
}
