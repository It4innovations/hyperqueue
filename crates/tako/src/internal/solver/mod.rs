#[cfg(feature = "highs")]
pub(crate) mod highs;
#[cfg(all(feature = "microlp", not(feature = "highs")))]
pub(crate) mod microlp;

#[cfg(feature = "highs")]
pub(crate) type LpInnerSolverImpl = highs::HighsSolver;

#[cfg(all(feature = "microlp", not(feature = "highs")))]
pub(crate) type LpInnerSolverImpl = microlp::MicrolpSolver;

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
    type Solution: LpSolution;

    fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Self::Variable;
    fn add_bool_variable(&mut self, weight: f64) -> Self::Variable;
    fn add_nat_variable(&mut self, weight: f64) -> Self::Variable;
    fn add_constraint(
        &mut self,
        constraint_type: ConstraintType,
        value: f64,
        variables: impl Iterator<Item = (Self::Variable, f64)>,
    );
    fn solve(self) -> Option<(Self::Solution, f64)>;
}

pub(crate) trait LpSolution {
    fn get_values(&self) -> &[f64];
}

pub(crate) struct LpSolver {
    solver: LpInnerSolverImpl,

    #[cfg(debug_assertions)]
    verbose: bool,
    #[cfg(debug_assertions)]
    var_name_map: crate::Map<Variable, usize>,
    #[cfg(debug_assertions)]
    name_config: Option<String>,
    #[cfg(debug_assertions)]
    variables: Vec<(String, f64)>,
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
            self.variables.push((name, weight));
        }
        variable
    }

    pub fn new(verbose: bool) -> Self {
        LpSolver {
            verbose,
            solver: LpInnerSolverImpl::new(),
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
            let name = &self.variables[*self.var_name_map.get(var).unwrap()].0;
            write!(
                &mut s,
                "{}{}*{}",
                if i == 0 {
                    ""
                } else {
                    if *weight < 0.0 { " - " } else { " + " }
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
    pub fn solve(self) -> Option<(Solution, f64)> {
        let s = self.solver.solve();
        if let Some((s, _)) = &s
            && self.verbose
        {
            println!("==== Solution: ====");
            for ((name, weight), value) in self.variables.iter().zip(s.get_values()) {
                if *weight == 0.0 {
                    println!("{} = {}", name, value);
                } else {
                    println!("{} = {} ({})", name, value, weight);
                }
            }
        }
        s
    }
}

#[cfg(not(debug_assertions))]
impl LpSolver {
    #[inline]
    pub fn set_name<F>(&mut self, create_name: F)
    where
        F: FnOnce() -> String,
    {
        // Do nothing
    }

    #[inline]
    fn new_var(&mut self, variable: Variable, weight: f64) -> Variable {
        variable
    }

    pub fn new(_verbose: bool) -> Self {
        LpSolver {
            solver: LpInnerSolverImpl::new(),
        }
    }

    #[inline]
    pub fn add_min_constraint(
        &mut self,
        min: f64,
        variables: impl Iterator<Item = (Variable, f64)>,
    ) {
        self.solver.add_min_constraint(min, variables)
    }

    #[inline]
    pub fn add_max_constraint(
        &mut self,
        max: f64,
        variables: impl Iterator<Item = (Variable, f64)>,
    ) {
        self.solver.add_max_constraint(max, variables)
    }

    #[inline]
    pub fn solve(self) -> Option<(Solution, f64)> {
        self.solver.solve()
    }
}

impl LpSolver {
    #[inline]
    pub fn add_variable(&mut self, weight: f64, min: f64, max: f64) -> Variable {
        let v = self.solver.add_variable(weight, min, max);
        self.new_var(v, weight)
    }

    #[inline]
    pub fn add_bool_variable(&mut self, weight: f64) -> Variable {
        let v = self.solver.add_bool_variable(weight);
        self.new_var(v, weight)
    }

    #[inline]
    pub fn add_nat_variable(&mut self, weight: f64) -> Variable {
        let v = self.solver.add_nat_variable(weight);
        self.new_var(v, weight)
    }
}
