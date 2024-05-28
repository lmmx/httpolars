use reqwest::blocking::Client;
use reqwest::Error;

pub fn make_request(foo: i32) -> Result<String, Error> {
    let client = Client::new();
    let url = "https://example.com/api";  // Replace with your actual endpoint

    let response = client
        .get(url)
        .query(&[("foo", foo)])
        .send()?
        .text()?;

    Ok(response)
}
