use crate::internal::solver::{ConstraintType, LpInnerSolver, LpSolution};
use highs::{HighsModelStatus, HighsSolutionStatus, Sense, Solution};
use std::time::Duration;

pub(crate) struct HighsSolver(highs::RowProblem);

impl HighsSolver {
    pub fn new() -> Self {
        HighsSolver(highs::RowProblem::new())
    }
}

impl LpInnerSolver for HighsSolver {
    type Variable = highs::Col;
    type Solution = HighsSolution;

    #[inline]
    fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Self::Variable {
        self.0.add_column(weight, min..max)
    }

    #[inline]
    fn add_bool_variable(&mut self, weight: f64) -> Self::Variable {
        self.0.add_integer_column(weight, 0..=1)
    }

    #[inline]
    fn add_nat_variable(&mut self, weight: f64) -> Self::Variable {
        self.0.add_integer_column(weight, 0..)
    }

    #[inline]
    fn add_constraint(
        &mut self,
        constraint_type: ConstraintType,
        value: f64,
        variables: impl Iterator<Item = (Self::Variable, f64)>,
    ) {
        match constraint_type {
            ConstraintType::Min => self.0.add_row(value.., variables),
            ConstraintType::Max => self.0.add_row(..=value, variables),
            ConstraintType::Eq => self.0.add_row(value..=value, variables),
        }
    }

    fn solve(self, time_limit: Option<Duration>) -> Option<Self::Solution> {
        let mut model = self.0.optimise(Sense::Maximise);
        if let Some(time_limit) = time_limit {
            model.set_option("time_limit", time_limit.as_secs_f64());
        }
        let solved_model = model.solve();
        let is_optimal = match solved_model.status() {
            HighsModelStatus::Optimal => true,
            HighsModelStatus::ModelEmpty => true,
            HighsModelStatus::ReachedTimeLimit
                if solved_model.primal_solution_status() == HighsSolutionStatus::Feasible =>
            {
                log::warn!(
                    "Scheduler MILP solve hit the {time_limit:?} time limit before proving \
                     optimality; dispatching the best incumbent found so far."
                );
                false
            }
            _ => return None,
        };
        let solution = solved_model.get_solution();

        Some(HighsSolution {
            solution,
            objective: solved_model.objective_value(),
            is_optimal,
        })
    }
}

pub(crate) struct HighsSolution {
    solution: Solution,
    objective: f64,
    is_optimal: bool,
}

impl LpSolution for HighsSolution {
    type Variable = highs::Col;

    #[inline]
    fn get_value(&self, v: highs::Col) -> f64 {
        self.solution[v]
    }

    #[inline]
    fn objective(&self) -> f64 {
        self.objective
    }

    #[inline]
    fn is_optimal(&self) -> bool {
        self.is_optimal
    }
}
