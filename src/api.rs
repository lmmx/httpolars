use reqwest::blocking::Client;
use reqwest::Error;
use std::collections::HashMap;

pub fn make_request(endpoint: &String, params: &HashMap<&str, &str>) -> Result<String, Error> {
    let client = Client::new();

    let response = client
        .get(endpoint)
        .query(&params)
        .send()?
        .text()?;

    Ok(response)
}
