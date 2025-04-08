use wasm_bindgen::prelude::*;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

// A macro to provide `println!(..)`-style syntax for `console.log` logging.
macro_rules! console_log {
    ($($t:tt)*) => (log(&format!($($t)*)))
}

#[wasm_bindgen]
pub struct EventClient {
    server_address: String,
}

#[wasm_bindgen]
impl EventClient {
    #[wasm_bindgen(constructor)]
    pub fn new(server_address: String) -> Self {
        console_error_panic_hook::set_once();
        console_log!("Creating EventClient with server address: {}", server_address);
        Self { server_address }
    }

    #[wasm_bindgen]
    pub fn get_info(&self) -> String {
        format!("EventClient connected to: {}", self.server_address)
    }
}

// This is a temporary simplified version to test WASM compilation
// The full implementation with gRPC client functionality will be added back
// once we resolve the dependency issues
//
// NOTE: The protobuf modules have been commented out temporarily 