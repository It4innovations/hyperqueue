#![allow(unused)]

use std::ops::{Deref, DerefMut};
use std::time::Duration;

use pyo3::types::{PyFloat, PyInt};
use pyo3::{Borrowed, Bound, FromPyObject, PyAny, PyErr, PyResult};
use pythonize::depythonize;
use serde::de::DeserializeOwned;

/// Wrapper type that implements deserialization from Python type using `serde::DeserializeOwned`.
#[derive(Debug)]
pub struct FromPy<T>(T);

impl<T> FromPy<T> {
    pub fn extract(self) -> T {
        self.0
    }
}

impl<T> Deref for FromPy<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for FromPy<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a, 'py, T> FromPyObject<'a, 'py> for FromPy<T>
where
    T: DeserializeOwned,
{
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        depythonize(&obj).map(|v| FromPy(v)).map_err(|e| e.into())
    }
}
