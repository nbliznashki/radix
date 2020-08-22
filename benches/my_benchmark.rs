use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use radix::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rayon::prelude::*;
use std::collections::hash_map::RandomState;

fn partition_and_flatten(c: &mut Criterion) {
    let strvec = prep_data(1_000, 7);

    let sample_data: Vec<StringVec> = [1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000]
        .iter()
        .map(|i| prep_data(*i, 7))
        .collect();

    let mut group = c.benchmark_group("partition and flatten");
    sample_data.iter().for_each(|strvec| {
        let len = strvec.strvec.len() as u64;
        group.throughput(Throughput::Bytes(len));
        group.bench_with_input(BenchmarkId::from_parameter(len), &strvec, |b, &strvec| {
            b.iter(|| performance_test(&strvec))
        });
    });

    group.finish();
}

fn prep_data(sample_size: usize, max_str_len: usize) -> StringVec {
    let strvec: Vec<String> = (0..sample_size)
        .into_par_iter()
        .map(|i| {
            let s: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(i % (max_str_len + 1))
                .collect();
            s
        })
        .collect();

    let strvec = StringVec { strvec };
    strvec
}
fn performance_test(strvec: &StringVec) {
    let s = RandomState::new();

    let hash = strvec.hash_column(&s, None);

    let b = BucketColumn::from_hash(hash, 10);
    let bmap = BucketsSizeMap::from_bucket_column(b, 2);

    let part = strvec.partition_column(&bmap);
    let part_index = match &part {
        PartitionedColumn::VariableLenType(columnu8) => {
            let v: ColumnIndexPartitioned = columnu8.par_iter().map(|_| None).collect();
            v
        }

        _ => panic!(),
    };

    //println!("{:?}", part);

    let flattened_index = part.flatten_index(&part_index);

    let flattened_column = part.flatten(&flattened_index);
    match flattened_column {
        FlattenedColumn::FixedLenType(_) => panic![],
        _ => {}
    };
}

criterion_group!(benches, partition_and_flatten);
criterion_main!(benches);
