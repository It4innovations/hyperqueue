#[cfg(all(feature = "coin_cbc", not(feature = "microlp"), not(feature = "highs")))]
pub(crate) mod coin_cbc;
#[cfg(feature = "highs")]
pub(crate) mod highs;
#[cfg(all(feature = "microlp", not(feature = "highs")))]
pub(crate) mod microlp;

use std::time::Duration;

#[cfg(feature = "highs")]
pub(crate) type LpInnerSolverImpl = highs::HighsSolver;

#[cfg(all(feature = "microlp", not(feature = "highs")))]
pub(crate) type LpInnerSolverImpl = microlp::MicrolpSolver;

#[cfg(all(feature = "coin_cbc", not(feature = "microlp"), not(feature = "highs")))]
pub(crate) type LpInnerSolverImpl = coin_cbc::CoinCbcSolver;

pub(crate) type Variable = <LpInnerSolverImpl as LpInnerSolver>::Variable;
pub(crate) type Solution = <LpInnerSolverImpl as LpInnerSolver>::Solution;

#[derive(Debug, Copy, Clone)]
pub(crate) enum ConstraintType {
    Min,
    Max,
    Eq,
}

pub(crate) trait LpInnerSolver {
    type Variable: Copy;
    type Solution: LpSolution<Variable = Self::Variable>;

    fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Self::Variable;
    fn add_bool_variable(&mut self, weight: f64) -> Self::Variable;
    fn add_nat_variable(&mut self, weight: f64) -> Self::Variable;
    fn add_constraint(
        &mut self,
        constraint_type: ConstraintType,
        value: f64,
        variables: impl Iterator<Item = (Self::Variable, f64)>,
    );

    /// Like `solve`, but allowed to trade exactness for a hard wall-clock
    /// cap. Returns whether the solution is proven optimal. Backends without
    /// a tuned implementation fall back to the exact `solve`.
    fn solve(self, time_limit: Option<Duration>) -> Option<Self::Solution>;

    /// Write the solver's own log to `path`. The evaluation reads the anytime behaviour of a
    /// truncated solve from it. Backends without a log ignore it.
    fn set_log_file(&mut self, path: std::path::PathBuf) {
        let _ = path;
    }
}

pub(crate) trait LpSolution {
    type Variable: Copy;
    fn get_value(&self, v: Self::Variable) -> f64;
    fn objective(&self) -> f64;
    fn is_optimal(&self) -> bool;
}

pub(crate) struct LpSolver {
    solver: LpInnerSolverImpl,

    /// MILP size, counted in every build: the variable names below are debug-only, but the
    /// evaluation measures release builds.
    n_variables: u32,
    n_constraints: u32,

    #[cfg(debug_assertions)]
    verbose: bool,
    #[cfg(debug_assertions)]
    var_name_map: crate::Map<Variable, usize>,
    #[cfg(debug_assertions)]
    name_config: Option<String>,
    #[cfg(debug_assertions)]
    variables: Vec<(String, f64, Variable)>,
}

#[cfg(debug_assertions)]
impl LpSolver {
    #[inline]
    pub fn set_name<F>(&mut self, create_name: F)
    where
        F: FnOnce() -> String,
    {
        self.name_config = Some(create_name());
    }

    #[inline]
    fn new_var(&mut self, variable: Variable, weight: f64) -> Variable {
        let name = self.name_config.take();
        if let Some(name) = name {
            self.var_name_map.insert(variable, self.variables.len());
            self.variables.push((name, weight, variable));
        }
        variable
    }

    pub fn new(verbose: bool) -> Self {
        #[cfg(not(any(feature = "highs", feature = "microlp", feature = "coin_cbc")))]
        {
            compile_error!(
                "You have to enable either the `highs`, `microlp`, or `coin_cbc` feature using `cargo build ... --features <highs/microlp/coin_cbc>`"
            )
        }
        LpSolver {
            verbose,
            solver: LpInnerSolverImpl::new(),
            n_variables: 0,
            n_constraints: 0,
            var_name_map: Default::default(),
            variables: Default::default(),
            name_config: None,
        }
    }

    #[inline]
    pub fn add_constraint(
        &mut self,
        constraint_type: ConstraintType,
        value: f64,
        variables: impl Iterator<Item = (Variable, f64)>,
    ) {
        self.n_constraints += 1;
        if self.verbose {
            let vars: Vec<_> = variables.collect();
            self.print_constraint(&vars, constraint_type, value);
            self.solver
                .add_constraint(constraint_type, value, vars.into_iter())
        } else {
            self.solver
                .add_constraint(constraint_type, value, variables)
        }
    }

    fn print_constraint(&mut self, variable: &[(Variable, f64)], ct: ConstraintType, bound: f64) {
        use std::fmt::Write;
        let mut s = String::new();
        if let Some(name) = self.name_config.take() {
            write!(s, "{}\n    ", name).unwrap();
        }
        for (i, (var, weight)) in variable.iter().enumerate() {
            let name = self
                .var_name_map
                .get(var)
                .map(|_idx| {
                    self.variables[*self.var_name_map.get(var).unwrap()]
                        .0
                        .as_str()
                })
                .unwrap_or("??");
            write!(
                &mut s,
                "{}{}*{}",
                if i == 0 {
                    ""
                } else if *weight < 0.0 {
                    " - "
                } else {
                    " + "
                },
                if *weight >= 0.0 || i == 0 {
                    *weight
                } else {
                    -*weight
                },
                if name.is_empty() { "??" } else { name }
            )
            .unwrap();
        }
        write!(
            &mut s,
            " {} {}",
            match ct {
                ConstraintType::Min => ">=",
                ConstraintType::Max => "<=",
                ConstraintType::Eq => "==",
            },
            bound
        )
        .unwrap();
        println!("{}", s);
    }

    #[inline]
    pub fn solve(self, time_limit: Option<Duration>) -> Option<Solution> {
        if self.verbose {
            println!("Weights:");
            for (name, weight, _var) in self.variables.iter() {
                if *weight != 0.0 {
                    println!("{} -> {}", name, weight);
                }
            }
        }
        let s = self.solver.solve(time_limit);
        if let Some(s) = &s
            && self.verbose
        {
            println!("==== Solution: ====");
            for (name, _weight, var) in self.variables.iter() {
                println!("{} = {}", name, s.get_value(*var));
            }
        }
        s
    }
}

#[cfg(not(debug_assertions))]
impl LpSolver {
    #[inline]
    pub fn set_name<F>(&mut self, _create_name: F)
    where
        F: FnOnce() -> String,
    {
        // Do nothing
    }

    #[inline]
    fn new_var(&mut self, variable: Variable, _weight: f64) -> Variable {
        variable
    }

    pub fn new(_verbose: bool) -> Self {
        LpSolver {
            solver: LpInnerSolverImpl::new(),
            n_variables: 0,
            n_constraints: 0,
        }
    }

    #[inline]
    pub fn add_constraint(
        &mut self,
        constraint_type: ConstraintType,
        value: f64,
        variables: impl Iterator<Item = (Variable, f64)>,
    ) {
        self.n_constraints += 1;
        self.solver
            .add_constraint(constraint_type, value, variables)
    }

    #[inline]
    pub fn solve(self, time_limit: Option<Duration>) -> Option<Solution> {
        self.solver.solve(time_limit)
    }
}

impl LpSolver {
    /// Number of variables added to the model.
    #[inline]
    pub fn n_variables(&self) -> u32 {
        self.n_variables
    }

    /// Number of constraints added to the model.
    #[inline]
    pub fn n_constraints(&self) -> u32 {
        self.n_constraints
    }

    #[inline]
    pub fn set_log_file(&mut self, path: std::path::PathBuf) {
        self.solver.set_log_file(path);
    }

    #[inline]
    pub fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Variable {
        let v = self.solver.add_variable(weight, min, max);
        self.n_variables += 1;
        self.new_var(v, weight)
    }

    #[inline]
    pub fn add_bool_variable(&mut self, weight: f64) -> Variable {
        let v = self.solver.add_bool_variable(weight);
        self.n_variables += 1;
        self.new_var(v, weight)
    }

    #[inline]
    pub fn add_nat_variable(&mut self, weight: f64) -> Variable {
        let v = self.solver.add_nat_variable(weight);
        self.n_variables += 1;
        self.new_var(v, weight)
    }
}
