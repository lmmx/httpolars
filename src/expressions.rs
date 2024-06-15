#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;
use std::collections::HashMap;
use crate::api::{make_request, ApiError, ApiResponse};

#[derive(Deserialize)]
struct ApiCallKwargs {
    endpoint: String,
}

#[polars_expr(output_type=String)]
fn api_call(inputs: &[Series], kwargs: ApiCallKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];
    let name = s.name();
    let endpoint = &kwargs.endpoint;

    let response_texts = match s.dtype() {
        DataType::String => {
            let ca = s.str()?;
            let texts: Vec<Option<String>> = ca.into_iter().map(|opt_v| {
                opt_v.map_or(None, |v| {
                    let mut params = HashMap::new();
                    params.insert(name, v);
                    match make_request(endpoint, &params) {
                        Ok((text, status_code)) => {
                            let response = ApiResponse { text, status_code };
                            Some(serde_json::to_string(&response).unwrap())
                        },
                        Err(ApiError::ReqwestError(e)) => {
                            let response = ApiResponse { text: format!("Request Error: {}", e), status_code: 500 };
							Some(serde_json::to_string(&response).unwrap())
						},
						Err(ApiError::Non200Status { text, status }) => {
                            let response = ApiResponse { text, status_code: status };
                            Some(serde_json::to_string(&response).unwrap())
                        }
                    }
                })
            }).collect();
            StringChunked::from_iter(texts)
        },
        DataType::Int32 => {
            let ca = s.i32()?;
            let texts: Vec<Option<String>> = ca.into_iter().map(|opt_v| {
                opt_v.map_or(None, |v| {
                    let mut params = HashMap::new();
                    let v_str = v.to_string();
                    params.insert(name, v_str.as_str());
                    match make_request(endpoint, &params) {
                        Ok((text, status_code)) => {
                            let response = ApiResponse { text, status_code };
                            Some(serde_json::to_string(&response).unwrap())
                        },
                        Err(ApiError::ReqwestError(e)) => {
                            let response = ApiResponse { text: format!("Request Error: {}", e), status_code: 500 };
							Some(serde_json::to_string(&response).unwrap())
						},
						Err(ApiError::Non200Status { text, status }) => {
                            let response = ApiResponse { text, status_code: status };
                            Some(serde_json::to_string(&response).unwrap())
                        }
                    }
                })
            }).collect();
            StringChunked::from_iter(texts)
        },
        DataType::Int64 => {
            let ca = s.i64()?;
            let texts: Vec<Option<String>> = ca.into_iter().map(|opt_v| {
                opt_v.map_or(None, |v| {
                    let mut params = HashMap::new();
                    let v_str = v.to_string();
                    params.insert(name, v_str.as_str());
                    match make_request(endpoint, &params) {
                        Ok((text, status_code)) => {
                            let response = ApiResponse { text, status_code };
                            Some(serde_json::to_string(&response).unwrap())
                        },
                        Err(ApiError::ReqwestError(e)) => {
                            let response = ApiResponse { text: format!("Request Error: {}", e), status_code: 500 };
							Some(serde_json::to_string(&response).unwrap())
						},
						Err(ApiError::Non200Status { text, status }) => {
                            let response = ApiResponse { text, status_code: status };
                            Some(serde_json::to_string(&response).unwrap())
                        }
                    }
                })
            }).collect();
            StringChunked::from_iter(texts)
        },
        dtype => polars_bail!(InvalidOperation:format!("Data type {dtype} not \
             supported for api_call, expected String, Int32, Int64.")),
    };

    // let struct_series = StructChunked::new(&[("response_text", response_texts), ("status_code", status_codes)])?.into_series();
    Ok(response_texts.into_series())
}
