use rand::{thread_rng, Rng};

pub fn generate_random_i32_array(size: usize) -> Vec<i32> {
    let mut rng = thread_rng();
    let mut array = Vec::with_capacity(size);

    for _ in 0..size {
        array.push(rng.gen_range(0..=100));
    }

    array
}

pub fn generate_random_boolean_array(size: usize) -> Vec<bool> {
    let mut rng = thread_rng();

    let array: Vec<bool> = (0..size).map(|_| rng.gen::<bool>()).collect();

    array
}

pub fn generate_boolean_array_all_true(size: usize) -> Vec<bool> {
    let array: Vec<bool> = (0..size).map(|_| true).collect();

    array
}

pub fn generate_boolean_array_all_false(size: usize) -> Vec<bool> {
    let array: Vec<bool> = (0..size).map(|_| true).collect();

    array
}
