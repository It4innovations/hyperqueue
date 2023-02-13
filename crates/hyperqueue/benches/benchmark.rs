use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion};
use hyperqueue::common::placeholders::{has_placeholders, parse_resolvable_string};

fn bench_parse_placeholder(c: &mut BenchmarkGroup<WallTime>) {
    c.bench_function("no placeholders", |bencher| {
        bencher.iter(|| {
            parse_resolvable_string("/tmp/my-very-long-path/that-is-even-longer-than-we-thought")
        });
    });
    c.bench_function("single placeholder", |bencher| {
        bencher.iter(|| {
            parse_resolvable_string(
                "/tmp/my-very-long-path/%{TASK_ID}/that-is-even-longer-than-we-thought",
            )
        });
    });
    c.bench_function("has_placeholders without placeholder", |bencher| {
        bencher.iter(|| {
            has_placeholders("/tmp/my-very-long-path/that-is-even-longer-than-we-thought")
        });
    });
    c.bench_function("has_placeholders with placeholder", |bencher| {
        bencher.iter(|| {
            has_placeholders(
                "/tmp/my-very-long-path/that-is-even-longer-than-we-thought/%{TASK_ID}",
            )
        });
    });
}

pub fn benchmark_placeholders(c: &mut Criterion) {
    let mut group = c.benchmark_group("placeholder");
    bench_parse_placeholder(&mut group);
}

criterion_group!(placeholders, benchmark_placeholders);

criterion_main!(placeholders);
