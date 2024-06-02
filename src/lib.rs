use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use reqwest::header::{HeaderMap, HeaderValue, HeaderName};
use reqwest::Client;
use std::collections::HashMap;

mod expressions;
mod utils;
mod api;

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;

use pyo3::types::PyModule;
use pyo3::{pymodule, PyResult, Python};

#[pyclass]
struct ApiClient {
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
fn create_api_client(headers: Option<HashMap<String, String>>) -> PyResult<ApiClient> {
    Ok(ApiClient::new(headers))
}

#[pymodule]
fn _lib(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<ApiClient>()?;
    m.add_function(wrap_pyfunction!(create_api_client, m)?)?;
    Ok(())
}
