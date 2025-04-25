use hyperqueue::common::error::HqError;
use pyo3::exceptions::PyException;
use pyo3::{PyErr, Python};

pub(crate) trait ToPyError {
    fn to_py(self) -> PyErr;
}

impl ToPyError for HqError {
    fn to_py(self) -> PyErr {
        Python::with_gil(|py| {
            PyErr::from_value(
                PyException::new_err(format!("{self:?}"))
                    .value(py)
                    .clone()
                    .into_any(),
            )
        })
    }
}

impl ToPyError for tako::Error {
    fn to_py(self) -> PyErr {
        HqError::from(self).to_py()
    }
}

pub(crate) trait ToPyResult<T> {
    fn map_py_err(self) -> Result<T, PyErr>;
}

impl<T, E> ToPyResult<T> for Result<T, E>
where
    E: ToPyError,
{
    fn map_py_err(self) -> Result<T, PyErr> {
        self.map_err(|e| e.to_py())
    }
}
