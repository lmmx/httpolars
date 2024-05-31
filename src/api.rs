use reqwest::blocking::Client;
use reqwest::Error;
use std::collections::HashMap;
use std::fmt;
use serde::Serialize;

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

pub fn make_request(endpoint: &String, params: &HashMap<&str, &str>) -> Result<(String, u16), ApiError> {
    let client = Client::new();

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
