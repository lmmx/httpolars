use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

mod expressions;
mod utils;
mod api;

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;

#[pymodule]
fn _lib(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<api::ApiClient>()?;
    m.add_function(wrap_pyfunction!(api::create_api_client, m)?)?;
    Ok(())
}
