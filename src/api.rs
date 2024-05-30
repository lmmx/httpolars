use reqwest::blocking::Client;
use reqwest::Error;
use std::collections::HashMap;

pub fn make_request(endpoint: &String, params: &HashMap<&str, &str>) -> Result<(String, u16), Error> {
    let client = Client::new();

    let response = client
        .get(endpoint)
        .query(&params)
        .send()?;
    
    let status = response.status().as_u16();
    let text = response.text()?;

    Ok((text, status))
}
