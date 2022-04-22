pub fn get_hostname(preferred: Option<String>) -> String {
    preferred.unwrap_or_else(|| {
        gethostname::gethostname()
            .into_string()
            .expect("Invalid hostname")
    })
}
