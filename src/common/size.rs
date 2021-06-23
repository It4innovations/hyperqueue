pub fn human_size(size: u64) -> String {
    if size < 2048 {
        format!("{} B", size)
    } else if size < 2 * 1024 * 1024 {
        format!("{} KiB", size / 1024)
    } else if size < 2 * 1024 * 1024 * 1024 {
        format!("{} MiB", size / (1024 * 1024))
    } else {
        format!("{} GiB", size / (1024 * 1024 * 1024))
    }
}

#[cfg(test)]
mod tests {
    use crate::common::size::human_size;

    #[test]
    fn test_sizes() {
        assert_eq!(human_size(0).as_str(), "0 B");
        assert_eq!(human_size(1).as_str(), "1 B");
        assert_eq!(human_size(1230).as_str(), "1230 B");
        assert_eq!(human_size(300_000).as_str(), "292 KiB");
        assert_eq!(human_size(50_000_000).as_str(), "47 MiB");
        assert_eq!(human_size(500_250_000_000).as_str(), "465 GiB");
    }
}
