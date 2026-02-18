use crate::internal::solver::ConstraintType;
use crate::internal::worker::resources::solver::{LpSolution, LpSolver};
use microlp::ComparisonOp;

pub(crate) struct MicrolpSolver(microlp::Problem);

impl MicrolpSolver {
    pub fn new() -> Self {
        MicrolpSolver(microlp::Problem::new(
            microlp::OptimizationDirection::Maximize,
        ))
    }
}

impl LpSolver for MicrolpSolver {
    type Variable = microlp::Variable;
    type Solution = MicrolpSolution;

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
        self.0.add_integer_var(weight, 0, i32::MAX)
    }

    #[inline]
    fn add_constraint(
        &mut self,
        constraint_type: ConstraintType,
        min: f64,
        variables: impl Iterator<Item = (Self::Variable, f64)>,
    ) {
        self.0.add_constraint(
            variables,
            match constraint_type {
                ConstraintType::Min => ComparisonOp::Ge,
                ConstraintType::Max => ComparisonOp::Le,
                ConstraintType::Rq => ComparisonOp::Eq,
            },
            min,
        )
    }

    fn solve(self) -> Option<(Self::Solution, f64)> {
        let Ok(solution) = self.0.solve() else {
            return None;
        };
        Some((
            MicrolpSolution(solution.iter().map(|x| *x.1).collect()),
            solution.objective(),
        ))
    }
}

pub(crate) struct MicrolpSolution(Vec<f64>);

impl LpSolution for MicrolpSolution {
    fn get_values(&self) -> &[f64] {
        self.0.as_slice()
    }
}
