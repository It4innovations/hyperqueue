use pyo3::{FromPyObject, PyAny, PyResult};
use pythonize::depythonize;
use serde::de::DeserializeOwned;
use std::ops::{Deref, DerefMut};

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

impl<'source, T> FromPyObject<'source> for FromPy<T>
where
    T: DeserializeOwned,
{
    fn extract(obj: &'source PyAny) -> PyResult<Self> {
        depythonize(obj).map(|v| FromPy(v)).map_err(|e| e.into())
    }
}
