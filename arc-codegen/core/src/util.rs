use crate::error::ErrorKind::*;
use crate::error::*;
use crate::module::*;
use weld::data::*;
use weld::*;

// TODO: There is probably a better way..
/// Turns a WeldVec of i8's into a Rust Vec
pub fn weld_to_raw(input: WeldVec<i8>) -> Result<Vec<i8>> {
    let boxed: Box<[i8]> = unsafe {
        Vec::from_raw_parts(
            input.data as *mut i8,
            input.len as usize,
            input.len as usize,
        )
        .into_boxed_slice()
    };
    let vec = boxed.clone().into_vec();
    std::mem::forget(boxed);
    Ok(vec)
}

/// Helper for creating a input serializer module
pub fn serialize_module_fmt(code: String) -> Result<String> {
    let re = regex::Regex::new(r"\|(.*?)\|").unwrap();
    if let Some(caps) = re.captures(&code) {
        let args = caps.get(1).map_or("", |m| m.as_str());
        let split_args: Vec<&str> = args.split(",").collect();

        let mut serialize_fn = String::new();
        serialize_fn += "serialize({";

        for (i, arg) in split_args.iter().enumerate() {
            let name: Vec<&str> = arg.trim().split(":").collect();
            if name.len() > 0 {
                serialize_fn += name[0];
                if i == split_args.len() - 1 {
                    serialize_fn += "})"
                } else {
                    serialize_fn += ",";
                }
            } else {
                return Err(Error::new(IOError(
                    "Failed to format module args".to_string(),
                )));
            }
        }

        let mut final_output = String::new();
        final_output += "|";
        final_output += args;
        final_output += "|";
        final_output += &serialize_fn;
        Ok(final_output)
    } else {
        Err(Error::new(IOError(
            "Failed to match regex for args".to_string(),
        )))
    }
}

// Converts a "normal" Weld module to accept raw bytes instead
// i.e., vec[i8]
pub fn generate_raw_module(code: String, serialize_ouput: bool) -> Result<String> {
    let mut module_code = String::new();
    module_code += "|raw: vec[i8]| ";
    let re = regex::Regex::new(r"\|(.*?)\|(.*)").unwrap();
    if let Some(caps) = re.captures(&code) {
        // TODO: Verify these
        let args = caps.get(1).map_or("", |m| m.as_str());
        let rest = caps.get(2).map_or("", |m| m.as_str());
        let split_args: Vec<&str> = args.split(",").collect();

        let mut args_info = Vec::<(String, String)>::new();

        for arg in split_args.iter() {
            let name: Vec<&str> = arg.trim().split(":").collect();
            if name.len() > 1 {
                args_info.push((name[0].to_string(), name[1].to_string()));
            } else {
                return Err(Error::new(IOError(
                    "Failed to format module args".to_string(),
                )));
            }
        }

        module_code += "let res = deserialize[{";
        for (i, arg) in args_info.iter().enumerate() {
            let arg_type = &arg.1;
            module_code += arg_type.trim();
            if i == args_info.len() - 1 {
                module_code += "}]"
            } else {
                module_code += ",";
            }
        }
        module_code += "(raw);\n";

        for (i, arg) in args_info.iter().enumerate() {
            let arg_name = &arg.0;
            module_code +=
                &("let ".to_owned() + arg_name.trim() + " = res.$" + &i.to_string() + ";\n");
        }

        // NOTE: this is to make sure the output of the weld module is serialized
        //       into raw bytes. It is an optional feature.
        if serialize_ouput == true {
            let re = regex::Regex::new(r"(.*?)([^;]*$)").unwrap();
            if let Some(caps) = re.captures(&rest.trim()) {
                let fn_code = caps.get(1).map_or("", |m| m.as_str());
                let output = caps.get(2).map_or("", |m| m.as_str());

                module_code += fn_code;
                module_code += "serialize(";
                module_code += output.trim();
                module_code += ")";
            } else {
                return Err(Error::new(IOError("Failed to catch regex".to_string())));
            }
        } else {
            module_code += rest.trim();
        }
    }
    Ok(module_code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_input_serialize_test() {
        let weld_code = String::from("|v: vec[i32], x: i32| x");
        let result = serialize_module_fmt(weld_code).unwrap();
        assert_eq!(result, "|v: vec[i32], x: i32|serialize({v,x})".to_string());
        // Verify that we can actually compile the module
        #[repr(C)]
        struct MyArgs {
            a: WeldVec<i32>,
            b: i32,
        }
        let id = String::from("module_ser_test");
        let priority = 0;
        let input_vec: Vec<i32> = [2, 3, 4, 2, 1].to_vec();
        let ref input_data = MyArgs {
            a: WeldVec::from(&input_vec),
            b: 1,
        };
        let module = Module::new(id, result, priority, None).unwrap();
        let ref mut ctx = WeldContext::new(&module.conf()).unwrap();
        let serialized_input: Result<ModuleRun<WeldVec<i8>>> = module.run(input_data, ctx);
        assert_eq!(serialized_input.is_ok(), true);
    }

    #[test]
    fn module_deserialize_raw_test() {
        // Example taken from the weld tests
        let code = "|x:vec[i32], y:vec[i32]| map(zip(x,y), |e| e.$0 + e.$1)";
        let result = generate_raw_module(code.to_string(), true).unwrap();
        let expected = "|raw: vec[i8]| let res = deserialize[{vec[i32],vec[i32]}](raw);\nlet x = res.$0;\nlet y = res.$1;\nserialize(map(zip(x,y), |e| e.$0 + e.$1))";
        assert_eq!(result, expected);
        let id = String::from("raw_module_test");
        let priority = 0;
        let module = Module::new(id, result, priority, None);
        assert_eq!(module.is_ok(), true);
    }
}
