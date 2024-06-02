use pyo3::prelude::*;
use reqwest::Error;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, HeaderName};
use std::collections::HashMap;
use std::fmt;
use serde::Serialize;

//#[pyclass]
//struct ApiClient {
//    client: Client,
//}
//
//#[pymethods]
//impl ApiClient {
//    #[new]
//    fn new() -> Self {
//        ApiClient {
//            client: Client::new(),
//        }
//    }
//}


#[pyclass]
pub struct ApiClient {
    client: Client,
    headers: HeaderMap,
}


#[pymethods]
impl ApiClient {
    #[new]
    fn new(headers: Option<HashMap<String, String>>) -> Self {
        let mut header_map = HeaderMap::new();
        if let Some(headers) = headers {
            for (key, value) in headers {
                let header_name = key.parse::<HeaderName>().unwrap();
                header_map.insert(header_name, HeaderValue::from_str(&value).unwrap());
            }
        }

        let client = Client::builder()
            .default_headers(header_map.clone())
            .build()
            .unwrap();

        ApiClient {
            client,
            headers: header_map,
        }
    }

    fn set_headers(&mut self, headers: HashMap<String, String>) {
        self.headers.clear();
        for (key, value) in headers {
            let header_name = key.parse::<HeaderName>().unwrap();
            self.headers.insert(header_name, HeaderValue::from_str(&value).unwrap());
        }

        self.client = Client::builder()
            .default_headers(self.headers.clone())
            .build()
            .unwrap();
    }
}

#[pyfunction]
pub fn create_api_client(headers: Option<HashMap<String, String>>) -> PyResult<ApiClient> {
    Ok(ApiClient::new(headers))
}

#[derive(Debug)]
pub enum ApiError {
    ReqwestError(Error),
    Non200Status { status: u16, text: String },
}

#[derive(Serialize)]
pub struct ApiResponse {
    pub text: String,
    pub status_code: u16,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::Non200Status { status, text } => write!(f, "Response error {}: {}", status, text),
            ApiError::ReqwestError(e) => write!(f, "Request error: non-200 status code {}", e),
        }
    }
}

impl std::error::Error for ApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApiError::ReqwestError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<Error> for ApiError {
    fn from(err: Error) -> Self {
        ApiError::ReqwestError(err)
    }
}

pub fn make_request(
    endpoint: &String,
    params: &HashMap<&str, &str>,
    client: Option<&PyCell<ApiClient>>,
) -> Result<(String, u16), ApiError> {
    let default_client;
    let client = match client {
        Some(api_client) => &api_client.borrow().client,
        None => {
            default_client = Client::new();
            &default_client
        }
    };

    let response_result = client
        .get(endpoint)
        .query(&params)
        .send();
    
    match response_result {
        Ok(response) => {
            let status = response.status().as_u16();
            if status != 200 {
                // I don't think this is ever used?
                let text = response.text().unwrap_or_else(|_| "Failed to read response text".to_string());
                return Err(ApiError::Non200Status { text, status });
            }
            let text = response.text()?;
            Ok((text, status))
        }
        Err(err) => {
            Err(ApiError::ReqwestError(err))
        },
    }
}
