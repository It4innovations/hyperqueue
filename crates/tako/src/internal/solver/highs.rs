use crate::internal::solver::{ConstraintType, LpInnerSolver, LpSolution};
use highs::{HighsModelStatus, HighsSolutionStatus, Sense};
use std::time::Duration;

pub(crate) struct HighsSolver(highs::RowProblem);

impl HighsSolver {
    pub fn new() -> Self {
        HighsSolver(highs::RowProblem::new())
    }
}

impl LpInnerSolver for HighsSolver {
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

    /// Unbounded, exact solve: used by the worker's own NUMA/socket resource
    /// allocator (see worker/resources/groups.rs), which relies on finding an
    /// exact feasible allocation rather than a merely-good-enough one -- these
    /// LPs are tiny (single-worker resource groups), so there is no
    /// scheduler-scale performance problem to trade off here.
    fn solve(self) -> Option<(Self::Solution, f64)> {
        let solved_model = self.0.optimise(Sense::Maximise).solve();
        if !matches!(solved_model.status(), HighsModelStatus::Optimal) {
            return None;
        }
        let solution = solved_model.get_solution();
        let objective_value = solved_model.objective_value();
        Some((solution, objective_value))
    }

    /// Bounded solve for the global task scheduler: hard-caps wall time at
    /// `time_limit` instead of always proving exact optimality. Returns
    /// whether the solution is proven optimal, since `solve_bounded` may
    /// dispatch a merely feasible incumbent on timeout.
    fn solve_bounded(self, time_limit: Duration) -> Option<(Self::Solution, bool)> {
        let mut model = self.0.optimise(Sense::Maximise);
        model.set_option("time_limit", time_limit.as_secs_f64());
        let solved_model = model.solve();

        let is_optimal = match solved_model.status() {
            HighsModelStatus::Optimal => true,
            // Time limit fired before optimality was proven. Dispatch the
            // incumbent anyway if it's feasible.
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
        Some((solution, is_optimal))
    }
}

impl LpSolution for highs::Solution {
    type Variable = highs::Col;

    #[inline]
    fn get_value(&self, v: highs::Col) -> f64 {
        self[v]
    }
}
