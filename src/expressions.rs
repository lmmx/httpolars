#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;
use std::collections::HashMap;
use crate::api::{make_request, ApiError, ApiResponse};
use reqwest::Client;
use tokio::runtime::Runtime;
use futures::future::join_all;

#[derive(Deserialize)]
struct ApiCallKwargs {
    endpoint: String,
}

#[tokio::main]
async fn handle_api_response(client: Client, endpoint: &String, params: &HashMap<&str, &str>) -> Option<String> {
    match make_request(client, endpoint, params).await {
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
}


#[polars_expr(output_type=String)]
fn api_call(inputs: &[Series], kwargs: ApiCallKwargs) -> PolarsResult<Series> {
    let s = &inputs[0];
    let name = s.name();
    let endpoint = &kwargs.endpoint;
    let client = Client::new();
    let response_texts = match s.dtype() {
        DataType::String => {
            let ca = s.str()?;
            let rt = Runtime::new().unwrap();
            let futures: Vec<_> = ca.into_iter().map(|opt_v| {
                let client = client.clone();
                let endpoint = endpoint.clone();
                tokio::spawn(async move {
                    match opt_v {
                        Some(v) => {
                            let mut params = HashMap::new();
                            params.insert(name.clone(), v);
                            handle_api_response(client, &endpoint, &params)
                        }
                        None => None
                    }
                })
            }).collect();
            let results = rt.block_on(async {
                join_all(futures).await
            });
            let texts: Vec<Option<String>> = results.into_iter().map(|res| {
                match res {
                    Ok(opt) => opt,
                    Err(_) => None, // Handle the join error if needed
                }
            }).collect();
            StringChunked::from_iter(texts)
        },
        DataType::Int32 => {
            let ca = s.i32()?;
            let rt = Runtime::new().unwrap();
            let futures: Vec<_> = ca.into_iter().map(|opt_v| {
                let client = client.clone();
                let endpoint = endpoint.clone();
                tokio::spawn(async move {
                    match opt_v {
                        Some(v) => {
                            let v_str = v.to_string();
                            let mut params = HashMap::new();
                            params.insert(name.clone(), v_str.as_str());
                            handle_api_response(client, &endpoint, &params)
                        }
                        None => None
                    }
                })
            }).collect();
            let results = rt.block_on(async {
                join_all(futures).await
            });
            let texts: Vec<Option<String>> = results.into_iter().map(|res| {
                match res {
                    Ok(opt) => opt,
                    Err(_) => None, // Handle the join error if needed
                }
            }).collect();
            StringChunked::from_iter(texts)
        },
        DataType::Int64 => {
            let ca = s.i64()?;
            let rt = Runtime::new().unwrap();
            let futures: Vec<_> = ca.into_iter().map(|opt_v| {
                let client = client.clone();
                let endpoint = endpoint.clone();
                tokio::spawn(async move {
                    match opt_v {
                        Some(v) => {
                            let v_str = v.to_string();
                            let mut params = HashMap::new();
                            params.insert(name.clone(), v_str.as_str());
                            handle_api_response(client, &endpoint, &params)
                        }
                        None => None
                    }
                })
            }).collect();
            let results = rt.block_on(async {
                join_all(futures).await
            });
            let texts: Vec<Option<String>> = results.into_iter().map(|res| {
                match res {
                    Ok(opt) => opt,
                    Err(_) => None, // Handle the join error if needed
                }
            }).collect();
            StringChunked::from_iter(texts)
        },
        dtype => polars_bail!(InvalidOperation:format!("Data type {dtype} not \
             supported for api_call, expected String, Int32, Int64.")),
    };

    // let struct_series = StructChunked::new(&[("response_text", response_texts), ("status_code", status_codes)])?.into_series();
    Ok(response_texts.into_series())
}
