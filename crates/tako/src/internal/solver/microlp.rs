use crate::internal::solver::{ConstraintType, LpInnerSolver, LpSolution};
use microlp::{ComparisonOp, Solution};

pub(crate) struct MicrolpSolver(microlp::Problem);

impl MicrolpSolver {
    pub fn new() -> Self {
        MicrolpSolver(microlp::Problem::new(
            microlp::OptimizationDirection::Maximize,
        ))
    }
}

impl LpInnerSolver for MicrolpSolver {
    type Variable = microlp::Variable;
    type Solution = Solution;

    #[inline]
    fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Self::Variable {
        self.0.add_var(weight, (min, max))
    }

    #[inline]
    fn add_bool_variable(&mut self, weight: f64) -> Self::Variable {
        self.0.add_binary_var(weight)
    }

    #[inline]
    fn add_nat_variable(&mut self, weight: f64) -> Self::Variable {
        // There is a known bug in micro-lp that large variable bounds produce incorrect results.
        // So we are setting some small but a reasonably large number.
        // Some of our tests fails even for a number like 40_000.
        // Btw: This (artificially) bounds a number of scheduled tasks of a single request on a single worker
        // If a user wants to schedule on a single worker more than 10_000 tasks, probably they should
        // not use microlp in the first place.
        self.0.add_integer_var(weight, (0, 10_000))
    }

    #[inline]
    fn add_constraint(
        &mut self,
        constraint_type: ConstraintType,
        value: f64,
        variables: impl Iterator<Item = (Self::Variable, f64)>,
    ) {
        self.0.add_constraint(
            variables,
            match constraint_type {
                ConstraintType::Min => ComparisonOp::Ge,
                ConstraintType::Max => ComparisonOp::Le,
                ConstraintType::Eq => ComparisonOp::Eq,
            },
            value,
        )
    }

    fn solve(self) -> Option<(Self::Solution, f64)> {
        let Ok(solution) = self.0.solve() else {
            return None;
        };
        let objective = solution.objective();
        Some((solution, objective))
    }
}

impl LpSolution for microlp::Solution {
    type Variable = microlp::Variable;

    #[inline]
    fn get_value(&self, v: microlp::Variable) -> f64 {
        *self.var_value(v)
    }
}
