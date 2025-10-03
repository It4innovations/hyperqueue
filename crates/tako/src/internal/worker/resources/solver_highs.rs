use crate::internal::worker::resources::solver::{LpSolution, LpSolver};
use highs::Sense;

pub(crate) struct HighsSolver(highs::RowProblem);

impl HighsSolver {
    pub fn new() -> Self {
        HighsSolver(highs::RowProblem::new())
    }
}

impl LpSolver for HighsSolver {
    type Variable = highs::Col;
    type Solution = highs::Solution;

    #[inline]
    fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Self::Variable {
        self.0.add_column(weight, min..max)
    }

    #[inline]
    fn add_bool_variable(&mut self, weight: f64) -> Self::Variable {
        self.0.add_integer_column(weight, 0..=1)
    }

    #[inline]
    fn add_constraint(&mut self, min: f64, variables: impl Iterator<Item = (Self::Variable, f64)>) {
        self.0.add_row(min.., variables);
    }

    fn solve(self) -> Option<(Self::Solution, f64)> {
        let solved_model = self.0.optimise(Sense::Maximise).solve();
        if !matches!(solved_model.status(), highs::HighsModelStatus::Optimal) {
            return None;
        }
        let solution = solved_model.get_solution();
        let objective_value = solved_model.objective_value();
        Some((solution, objective_value))
    }
}

impl LpSolution for highs::Solution {
    #[inline]
    fn get_values(&self) -> &[f64] {
        self.columns()
    }
}
