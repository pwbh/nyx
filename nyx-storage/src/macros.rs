// Creates a string with the name of the function it currently resides in
// this function is used in tests where we create distinct files not to have
// a race condition in tests running in parallel
#[allow(unused_macros)]
macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        name.strip_suffix("::f").unwrap().replace("::", "_")
    }};
}

#[allow(unused_imports)]
pub(crate) use function;
