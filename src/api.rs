use reqwest::blocking::Client;
use reqwest::Error;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug)]
pub enum ApiError {
    ReqwestError(Error),
    Non200Status { status: u16, text: String },
}


impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::ReqwestError(e) => write!(f, "Request error: {}", e),
            ApiError::Non200Status { status, text } => write!(f, "Error {}: {}", status, text),
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

pub fn make_request(endpoint: &String, params: &HashMap<&str, &str>) -> Result<String, ApiError> {
    let client = Client::new();

    let response = client
        .get(endpoint)
        .query(&params)
        .send()?;
    
    let status = response.status().as_u16();
    let text = response.text()?;

    if status != 200 {
        return Err(ApiError::Non200Status { status, text });
    }

    Ok(text)
}
