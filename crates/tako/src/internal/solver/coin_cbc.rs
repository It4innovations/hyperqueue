use crate::internal::solver::{ConstraintType, LpInnerSolver, LpSolution};
use coin_cbc::{Col, Model, Sense};

pub(crate) struct CoinCbcSolver {
    model: Model,
    cols: Vec<Col>,
}

impl CoinCbcSolver {
    pub fn new() -> Self {
        let mut model = Model::default();
        model.set_obj_sense(Sense::Maximize);
        CoinCbcSolver {
            model,
            cols: Vec::new(),
        }
    }
}

impl LpInnerSolver for CoinCbcSolver {
    type Variable = Col;
    type Solution = CoinCbcSolution;

    #[inline]
    fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Self::Variable {
        let col = self.model.add_col();
        self.model.set_obj_coeff(col, weight);
        self.model.set_col_lower(col, min);
        self.model.set_col_upper(col, max);
        self.cols.push(col);
        col
    }

    #[inline]
    fn add_bool_variable(&mut self, weight: f64) -> Self::Variable {
        let col = self.model.add_binary();
        self.model.set_obj_coeff(col, weight);
        self.cols.push(col);
        col
    }

    #[inline]
    fn add_nat_variable(&mut self, weight: f64) -> Self::Variable {
        let col = self.model.add_integer();
        self.model.set_obj_coeff(col, weight);
        self.model.set_col_lower(col, 0.0);
        self.cols.push(col);
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

    fn solve(self) -> Option<(Self::Solution, f64)> {
        let CoinCbcSolver { model, cols } = self;
        let solution = model.solve();
        if !solution.raw().is_proven_optimal() {
            return None;
        }
        let obj = solution.raw().obj_value();
        let values: Vec<f64> = cols.iter().map(|&col| solution.col(col)).collect();
        Some((CoinCbcSolution(values), obj))
    }
}

pub(crate) struct CoinCbcSolution(Vec<f64>);

impl LpSolution for CoinCbcSolution {
    fn get_values(&self) -> &[f64] {
        self.0.as_slice()
    }
}
