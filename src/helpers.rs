use rayon::prelude::*;
use std::{cmp::min, ops::AddAssign};

pub fn partial_sum_serial<T>(input: &[T], start_value: T) -> Vec<T>
where
    T: AddAssign,
    T: Clone,
{
    let mut output: Vec<T> = Vec::with_capacity(input.len());
    let mut current_val: T = start_value;

    input.iter().for_each(|v| {
        current_val += v.clone();
        output.push(current_val.clone());
    });
    output
}

pub fn partial_sum_serial_assign<T>(input: &mut [T], start_value: T)
where
    T: AddAssign,
    T: Clone,
{
    let mut current_val: T = start_value;

    input.iter_mut().for_each(|v| {
        current_val += v.clone();
        *v = current_val.clone();
    });
}

pub fn partial_sum_serial_assign_nostart<T>(input: &mut [T])
where
    T: AddAssign,
    T: Clone,
{
    (0..input.len() - 1).for_each(|i| input[i + 1] += input[i].clone());
}

pub fn partial_sum_serial_with_buffer<T>(input: &[T], output: &mut [T], start_value: T)
where
    T: AddAssign,
    T: Clone,
{
    assert_eq!(
        input.len(),
        output.len(),
        "Input and output slices have different length ",
    );

    let mut current_val: T = start_value;

    input
        .iter()
        .zip(output.iter_mut())
        .for_each(|(input_val, o)| {
            current_val += input_val.clone();
            *o = current_val.clone();
        });
}

pub fn partial_sum_serial_with_buffer_nostart<T>(input: &[T], output: &mut [T])
where
    T: AddAssign,
    T: Clone,
{
    assert_eq!(
        input.len(),
        output.len(),
        "Input and output slices have different length ",
    );

    if !input.is_empty() {
        output[0] = input[0].clone();
        (1..input.len()).for_each(|i| {
            output[i] = input[i].clone();
            output[i] += output[i - 1].clone();
        });
    }
}

pub fn partial_sum_parallel<T>(
    input: &[T],
    start_value: T,
    workers_count: std::num::NonZeroUsize,
) -> Vec<T>
where
    T: AddAssign,
    T: Clone,
    T: Send + Sync,
    T: std::fmt::Display + std::fmt::Debug,
{
    let mut output: Vec<T> = vec![start_value.clone(); input.len()];
    partial_sum_parallel_with_buffer(input, &mut output, start_value, workers_count);
    output
}

pub fn partial_sum_parallel_with_buffer<T>(
    input: &[T],
    output: &mut [T],
    start_value: T,
    workers_count: std::num::NonZeroUsize,
) where
    T: AddAssign,
    T: Clone,
    T: Send + Sync,
    T: std::fmt::Display + std::fmt::Debug,
{
    assert_eq!(
        input.len(),
        output.len(),
        "Input and output slices have different length ",
    );

    let mut p = workers_count.get();
    let n: usize = input.len();

    //If the input vector size is 0, then there is nothing to do
    if n == 0 {
        return;
    }

    //Divide the initial array in p+1 parts, where p is the worker count
    let chunk_size = ((n - 1) / (p + 1)) + 1;

    //If there is only 1 worker or the chunk size is 0, then run the serial version
    if p == 1 && chunk_size < 1 {
        return partial_sum_serial_with_buffer(input, output, start_value);
    }

    //Recalculate the worker count
    let p = (n + chunk_size - 1) / chunk_size - 1;

    //Start of parallel part
    //let mut output: Vec<T> = vec![start_value.clone(); input.len()];

    //If the chunk size is 0, then there is nothing to do

    //Calculate the partial sum separately in each of the first p chunks
    input
        .par_chunks(chunk_size)
        .zip_eq(output.par_chunks_mut(chunk_size))
        .take(p)
        .enumerate()
        .for_each(|(i, (input_slice, output_slice))| {
            if i == 0 {
                partial_sum_serial_with_buffer(input_slice, output_slice, start_value.clone())
            } else {
                partial_sum_serial_with_buffer_nostart(input_slice, output_slice);
            }
        });

    //Collect the last value of each chunk
    let mut chunk_last_value: Vec<T> = output
        .par_chunks_mut(chunk_size)
        .take(p)
        .map(|i| i[chunk_size - 1].clone())
        .collect();

    //And calculate partial sums
    partial_sum_serial_assign_nostart(&mut chunk_last_value);

    //The first chunk is done, there is nothing to do
    //For the ones between 2 and p, add the last value of the previous chunk to all the entries in the chunk
    //For the last chunk - calculate the prefix sum, adding the last value of the previous
    //chunk to the start value

    output
        .par_chunks_mut(chunk_size)
        .zip_eq(input.par_chunks(chunk_size))
        .enumerate()
        .skip(1)
        .zip_eq(chunk_last_value.par_iter().take(p))
        .for_each(|((i, (output_slice, input_slice)), add_on)| {
            if i < p {
                output_slice.iter_mut().for_each(|o| *o += add_on.clone());
            } else {
                //We simply add the previous chunk last value to the start value
                let new_start_value = add_on.clone();
                partial_sum_serial_with_buffer(input_slice, output_slice, new_start_value)
            }
        });
}
