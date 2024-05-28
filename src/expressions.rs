#![allow(clippy::unused_unit)]
use polars::prelude::*;
use polars::prelude::arity::binary_elementwise;
use pyo3_polars::derive::polars_expr;
use std::fmt::Write;
use serde::Deserialize;

#[polars_expr(output_type=String)]
fn pig_latinnify(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
        if let Some(first_char) = value.chars().next() {
            write!(output, "{}{}ay", &value[1..], first_char).unwrap()
        }
    });
    Ok(out.into_series())
}


#[polars_expr(output_type=Int64)]
fn abs_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca: &Int64Chunked = s.i64()?;
    // NOTE: there's a faster way of implementing `abs_i64`, which we'll
    // cover in section 7.
    let out: Int64Chunked = ca.apply(|opt_v: Option<i64>| opt_v.map(|v: i64| v.abs()));
    Ok(out.into_series())
}

#[polars_expr(output_type=Int64)]
fn sum_i64(inputs: &[Series]) -> PolarsResult<Series> {
    let left: &Int64Chunked = inputs[0].i64()?;
    let right: &Int64Chunked = inputs[1].i64()?;
    // Note: there's a faster way of summing two columns, see
    // section 7.
    let out: Int64Chunked = binary_elementwise(
        left,
        right,
        |left: Option<i64>, right: Option<i64>| match (left, right) {
            (Some(left), Some(right)) => Some(left + right),
            _ => None,
        },
    );
    Ok(out.into_series())
}

#[derive(Deserialize)]
struct AddSuffixKwargs {
    suffix: String,
}

#[polars_expr(output_type=String)]
fn add_suffix(inputs: &[Series], kwargs: AddSuffixKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;
    let out = ca.apply_to_buffer(|value, output| {
        write!(output, "{}{}", value, kwargs.suffix).unwrap();
    });
    Ok(out.into_series())
}

#[derive(Deserialize)]
struct ApiCallKwargs {
    endpoint: String,
}

#[polars_expr(output_type=String)]
fn api_call(inputs: &[Series], kwargs: ApiCallKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];

    let out = match s.dtype() {
        // DataType::Utf8 => write_endpoint(s.str().unwrap(), &kwargs.endpoint),
        DataType::Int32 => {
            let ca = s.i32()?;
            StringChunked::from_iter(
                ca.into_iter().map(|opt_v| opt_v.map(|v| format!("{}?param={}", &kwargs.endpoint, v))),
            ).into_series()
        },
        DataType::Int64 => {
            let ca = s.i64()?;
            StringChunked::from_iter(
                ca.into_iter().map(|opt_v| opt_v.map(|v| format!("{}?param={}", &kwargs.endpoint, v))),
            ).into_series()
        },
        dtype => polars_bail!(InvalidOperation:format!("Data type {dtype} not \
             supported for api_call, expected Utf8, Int32, Int64.")),
    };

    Ok(out.into_series())
}
