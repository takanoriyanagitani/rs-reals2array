pub use arrow;

use arrow::array::PrimitiveArray;
use arrow::array::PrimitiveBuilder;

use arrow::datatypes::ArrowPrimitiveType;

pub trait FloatType: ArrowPrimitiveType {
    fn is_nan(val: Self::Native) -> bool;
}

impl FloatType for arrow::datatypes::Float16Type {
    fn is_nan(val: Self::Native) -> bool {
        val.is_nan()
    }
}

impl FloatType for arrow::datatypes::Float32Type {
    fn is_nan(val: Self::Native) -> bool {
        val.is_nan()
    }
}

impl FloatType for arrow::datatypes::Float64Type {
    fn is_nan(val: Self::Native) -> bool {
        val.is_nan()
    }
}

pub fn is_nan<N>(num: N::Native) -> bool
where
    N: FloatType,
{
    N::is_nan(num)
}

pub fn nan2none<N>(num: N::Native) -> Option<N::Native>
where
    N: FloatType,
{
    let nan: bool = is_nan::<N>(num);
    let ok: bool = !nan;
    ok.then_some(num)
}

pub fn num2builder<T>(num: T::Native, bldr: &mut PrimitiveBuilder<T>)
where
    T: FloatType,
{
    let o: Option<T::Native> = nan2none::<T>(num);
    match o {
        None => bldr.append_null(),
        Some(i) => bldr.append_value(i),
    }
}

pub fn opt2builder<T>(num: Option<T::Native>, bldr: &mut PrimitiveBuilder<T>)
where
    T: FloatType,
{
    let o: Option<T::Native> = num.and_then(nan2none::<T>);
    match o {
        None => bldr.append_null(),
        Some(i) => bldr.append_value(i),
    }
}

pub fn num2array<I, T>(num: I, cap: usize) -> PrimitiveArray<T>
where
    T: FloatType,
    I: Iterator<Item = T::Native>,
{
    let mut bldr = PrimitiveBuilder::with_capacity(cap);

    for n in num {
        num2builder(n, &mut bldr);
    }

    bldr.finish()
}

pub fn opt2array<I, T>(num: I, cap: usize) -> PrimitiveArray<T>
where
    T: FloatType,
    I: Iterator<Item = Option<T::Native>>,
{
    let mut bldr = PrimitiveBuilder::with_capacity(cap);

    for n in num {
        opt2builder(n, &mut bldr);
    }

    bldr.finish()
}

pub const CAPACITY_DEFAULT: usize = 1024;

pub fn num2array_default<I, T>(num: I) -> PrimitiveArray<T>
where
    T: FloatType,
    I: Iterator<Item = T::Native>,
{
    num2array(num, CAPACITY_DEFAULT)
}

pub fn opt2array_default<I, T>(num: I) -> PrimitiveArray<T>
where
    T: FloatType,
    I: Iterator<Item = Option<T::Native>>,
{
    opt2array(num, CAPACITY_DEFAULT)
}

macro_rules! num2arr {
    ($fname: ident, $ptyp: ty) => {
        /// Converts the numbers to an array.
        pub fn $fname<I>(num: I) -> PrimitiveArray<$ptyp>
        where
            I: Iterator<Item = <$ptyp as ArrowPrimitiveType>::Native>,
        {
            num2array_default(num)
        }
    };
}

num2arr!(num2arr16f, arrow::array::types::Float16Type);
num2arr!(num2arr32f, arrow::array::types::Float32Type);
num2arr!(num2arr64f, arrow::array::types::Float64Type);

macro_rules! opt2arr {
    ($fname: ident, $ptyp: ty) => {
        /// Converts the optionals to an array.
        pub fn $fname<I>(num: I) -> PrimitiveArray<$ptyp>
        where
            I: Iterator<Item = Option<<$ptyp as ArrowPrimitiveType>::Native>>,
        {
            opt2array_default(num)
        }
    };
}

opt2arr!(opt2arr16f, arrow::array::types::Float16Type);
opt2arr!(opt2arr32f, arrow::array::types::Float32Type);
opt2arr!(opt2arr64f, arrow::array::types::Float64Type);

pub fn num2opt(n: &serde_json::Number) -> Option<f64> {
    let of: Option<f64> = n.as_f64();
    of.and_then(nan2none::<arrow::datatypes::Float64Type>)
}

pub fn val2opt(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => num2opt(n),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::array::Float32Array;

    #[test]
    fn test_num2arr32f_basic() {
        // 1.0, NaN, 3.5
        let data = vec![1.0f32, f32::NAN, 3.5f32];
        let arr: Float32Array = num2arr32f(data.into_iter());

        assert_eq!(arr.len(), 3);
        assert_eq!(arr.null_count(), 1);

        // Valid indices
        assert!(arr.is_valid(0));
        assert!(!arr.is_valid(1)); // NaN should become null
        assert!(arr.is_valid(2));

        // Values
        assert_eq!(arr.value(0), 1.0);
        assert_eq!(arr.value(2), 3.5);
    }

    #[test]
    fn test_num2arr32f_empty_and_all_nan() {
        // Empty iterator
        let empty_arr: Float32Array = num2arr32f(std::iter::empty());
        assert_eq!(empty_arr.len(), 0);
        assert_eq!(empty_arr.null_count(), 0);

        // All NaN
        let nan_vec = vec![f32::NAN; 5];
        let nan_arr: Float32Array = num2arr32f(nan_vec.into_iter());
        assert_eq!(nan_arr.len(), 5);
        assert_eq!(nan_arr.null_count(), 5);
        for i in 0..5 {
            assert!(!nan_arr.is_valid(i));
        }
    }
}
