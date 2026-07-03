use crate::internal::solver::config::{mip_rel_gap, mip_time_limit};
use crate::internal::solver::{ConstraintType, LpInnerSolver, LpSolution};
use highs::{HighsModelStatus, HighsSolutionStatus, Sense};

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

    /// Bounded solve for the global task scheduler: accepts a solution once
    /// it is provably within `mip_rel_gap` of optimal, and hard-caps wall
    /// time at `mip_time_limit`. Task resource requests are themselves
    /// estimates, and this solve can otherwise blow up (unboundedly, on the
    /// single-threaded server) with many distinct resource shapes, so a
    /// bounded solve is the right tradeoff here -- unlike `solve()`, which
    /// callers that need a guaranteed-exact answer (the resource allocator)
    /// must keep using instead.
    fn solve_bounded(self) -> Option<(Self::Solution, f64)> {
        let mut model = self.0.optimise(Sense::Maximise);
        model.set_option("time_limit", mip_time_limit().as_secs_f64());
        model.set_option("mip_rel_gap", mip_rel_gap());
        let solved_model = model.solve();

        match solved_model.status() {
            // Either an exact optimum, or (since mip_rel_gap is set) a
            // solution HiGHS has proven is within the accepted gap of
            // optimal.
            HighsModelStatus::Optimal => {}
            // The hard wall-clock cap fired before the gap could be proven.
            // Still dispatch it if it's a real feasible incumbent -- it
            // never violates a constraint, it's just not proven close to
            // optimal -- so a slow-converging solve degrades scheduling
            // quality for one pass instead of blocking the server.
            HighsModelStatus::ReachedTimeLimit
                if solved_model.primal_solution_status() == HighsSolutionStatus::Feasible =>
            {
                log::warn!(
                    "Scheduler MILP solve hit the {:?} time limit before proving the {:.0}% \
                     optimality gap; dispatching the best incumbent found so far.",
                    mip_time_limit(),
                    mip_rel_gap() * 100.0
                );
            }
            _ => return None,
        }

        let solution = solved_model.get_solution();
        let objective_value = solved_model.objective_value();
        Some((solution, objective_value))
    }
}

impl LpSolution for highs::Solution {
    type Variable = highs::Col;

    #[inline]
    fn get_value(&self, v: highs::Col) -> f64 {
        self[v]
    }
}
