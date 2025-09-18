pub(crate) trait LpSolver {
    type Variable: Copy;
    type Solution: LpSolution;

    fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Self::Variable;
    fn add_bool_variable(&mut self, weight: f64) -> Self::Variable;
    fn add_constraint(&mut self, min: f64, variables: impl Iterator<Item = (Self::Variable, f64)>);
    fn solve(self) -> Option<(Self::Solution, f64)>;
}

pub(crate) trait LpSolution {
    fn get_values(&self) -> &[f64];
}

#[inline]
pub fn create_solver() -> impl LpSolver {
    #[cfg(feature = "highs")]
    {
        use super::solver_highs::HighsSolver;
        HighsSolver::new()
    }
    #[cfg(all(feature = "microlp", not(feature = "highs")))]
    {
        use super::solver_microlp::MicrolpSolver;
        MicrolpSolver::new()
    }
}
