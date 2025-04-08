# TTP Consumer WebAssembly Implementation

This project is a WebAssembly version of the TTP (Token Transfer Protocol) consumer application. It allows you to consume TTP events from a gRPC service directly in a web browser or Node.js environment using WebAssembly.

## Prerequisites

To build this project, you need:

1. [Rust](https://www.rust-lang.org/tools/install) (latest stable version)
2. [wasm-pack](https://rustwasm.github.io/wasm-pack/installer/)
3. [Protobuf compiler](https://grpc.io/docs/protoc-installation/) (needed for gRPC code generation)

## Project Structure

- `/proto` - Protocol Buffer definitions
- `/src` - Rust source code
- `/web-example` - Example web application that uses the WebAssembly module

## Building

1. **Install wasm-pack if you haven't already**:
   ```bash
   curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
   ```

2. **Build the WebAssembly module**:
   ```bash
   wasm-pack build --target web
   ```
   
   This will create a `pkg` directory with the compiled WebAssembly module and JavaScript bindings.

3. **For Node.js target**:
   ```bash
   wasm-pack build --target nodejs
   ```

## Troubleshooting

### Web-sys Feature Flag Issues

If you encounter errors like:

```
error: failed to select a version for `web-sys`.
the package `ttp_consumer_wasm` depends on `web-sys`, with features: `fetch` but `web-sys` does not have these features.
```

This is because of a mismatch between the requested features and the available features in the selected version of `web-sys`. To fix this:

1. **Use the simplified version**:
   - The project currently includes a simplified version that removes the complex dependencies
   - This version doesn't include gRPC functionality but allows you to test the WASM compilation

2. **To restore full functionality**:
   1. Uncomment the code in `build.rs`
   2. Restore the full implementation in `src/lib.rs`
   3. Update Cargo.toml dependencies to use compatible versions:
      ```toml
      wasm-bindgen = "0.2.84"
      wasm-bindgen-futures = "0.4.34"
      js-sys = "0.3.61"
      web-sys = { version = "0.3.61", features = [
          "console",
      ] }
      tonic-web-wasm-client = "0.3.0"
      ```

## Usage in Web Applications

1. Copy the generated `pkg` directory to your web project.

2. Import and use the EventClient:

```javascript
import init, { EventClient } from './pkg/ttp_consumer_wasm.js';

// Initialize the WebAssembly module
async function run() {
  // Initialize the WASM module
  await init();
  
  // Create a client
  const client = new EventClient('http://localhost:50051');
  
  // Get info from the client
  console.log(client.get_info());
  
  // When full functionality is restored:
  /*
  // Get TTP events
  client.get_ttp_events(
    1,  // startLedger
    10, // endLedger
    (event) => {
      // Handle each event
      console.log('Received TTP event:', event);
    }
  ).then(() => {
    console.log('Stream completed');
  }).catch(error => {
    console.error('Error:', error.message);
  });
  */
}

run();
```

## Important Notes for Web Usage

1. **gRPC-Web Proxy**: For web browser usage, you need a gRPC-Web proxy (like Envoy) in front of your gRPC server, as browsers cannot make direct gRPC calls.

2. **CORS**: Ensure your gRPC-Web server has proper CORS headers configured to allow browser requests.

## Example Web Application

The `web-example` directory contains a simple HTML/JavaScript application that demonstrates how to use the WebAssembly module. To run it:

1. Build the WebAssembly module as described above.
2. Copy the `pkg` directory to `web-example/pkg`.
3. Serve the `web-example` directory with a web server:
   ```bash
   # Using Python's built-in server
   python -m http.server -d web-example
   
   # Or using Node.js with http-server
   npx http-server web-example
   ```

## Differences from the Node.js Implementation

The WebAssembly implementation has a few key differences from the original Node.js implementation:

1. It uses gRPC-Web instead of regular gRPC, which requires a proxy server.
2. The event handling is Promise-based rather than using Node.js streams.
3. Some features might be limited due to browser/WebAssembly constraints.

## License

Same as the original project. 