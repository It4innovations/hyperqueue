use crate::internal::solver::{ConstraintType, LpInnerSolver, LpSolution};
use coin_cbc::{Col, Model, Sense};
use std::time::Duration;

pub(crate) struct CoinCbcSolver {
    model: Model,
}

impl CoinCbcSolver {
    pub fn new() -> Self {
        let mut model = Model::default();
        model.set_parameter("log", "0");
        model.set_obj_sense(Sense::Maximize);
        CoinCbcSolver { model }
    }
}

impl LpInnerSolver for CoinCbcSolver {
    type Variable = Col;
    type Solution = coin_cbc::Solution;

    #[inline]
    fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Self::Variable {
        let col = self.model.add_col();
        self.model.set_obj_coeff(col, weight);
        self.model.set_col_lower(col, min);
        self.model.set_col_upper(col, max);
        col
    }

    #[inline]
    fn add_bool_variable(&mut self, weight: f64) -> Self::Variable {
        let col = self.model.add_binary();
        self.model.set_obj_coeff(col, weight);
        col
    }

    #[inline]
    fn add_nat_variable(&mut self, weight: f64) -> Self::Variable {
        let col = self.model.add_integer();
        self.model.set_obj_coeff(col, weight);
        self.model.set_col_lower(col, 0.0);
        col
    }

    #[inline]
    fn add_constraint(
        &mut self,
        constraint_type: ConstraintType,
        value: f64,
        variables: impl Iterator<Item = (Self::Variable, f64)>,
    ) {
        let row = self.model.add_row();
        match constraint_type {
            ConstraintType::Min => self.model.set_row_lower(row, value),
            ConstraintType::Max => self.model.set_row_upper(row, value),
            ConstraintType::Eq => self.model.set_row_equal(row, value),
        }
        for (col, coeff) in variables {
            self.model.set_weight(row, col, coeff);
        }
    }

    fn solve(mut self, time_limit: Option<Duration>) -> Option<Self::Solution> {
        if let Some(time_limit) = time_limit {
            self.model
                .set_parameter("seconds", &time_limit.as_secs_f64().to_string());
        }
        let solution = self.model.solve();
        if !solution.raw().is_proven_optimal() {
            return None;
        }
        Some(solution)
    }
}

impl LpSolution for coin_cbc::Solution {
    type Variable = Col;

    #[inline]
    fn get_value(&self, v: Col) -> f64 {
        self.col(v)
    }

    #[inline]
    fn objective(&self) -> f64 {
        self.raw().obj_value()
    }

    #[inline]
    fn is_optimal(&self) -> bool {
        self.raw().is_proven_optimal()
    }
}
