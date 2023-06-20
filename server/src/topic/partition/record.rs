use std::collections::HashMap;

pub enum Value {
    Str(String),
    Integer(i32),
    Float(f32),
    Bool(bool),
    Complex { key: String, value: Box<Value> },
}

pub type Payload = HashMap<String, Value>;

pub struct Record {
    id: String,
    payload: Payload,
}
