# Resolving Dependencies for WebAssembly Compilation

This guide provides solutions for resolving dependency conflicts when compiling the TTP Consumer to WebAssembly.

## Common Issues

### 1. Feature Flag Conflicts with `web-sys`

The most common error is related to feature flag conflicts with the `web-sys` crate:

```
error: failed to select a version for `web-sys`.
the package `ttp_consumer_wasm` depends on `web-sys`, with features: `fetch` but `web-sys` does not have these features.
```

This happens because `web-sys` is a large crate with many feature flags, and sometimes the version selected by Cargo doesn't have all the features we've requested.

### Solution:

1. **Pin specific versions** of all related crates to ensure compatibility:

```toml
wasm-bindgen = "=0.2.84"
wasm-bindgen-futures = "=0.4.34"
js-sys = "=0.3.61"
web-sys = { version = "=0.3.61", features = [
    "console",
    # Add only features available in this version
] }
```

2. **Check available features** for your selected version of `web-sys`:
   - Visit [docs.rs](https://docs.rs/web-sys/0.3.61/web_sys/) for your specific version
   - Look at the list of available features
   - Only include features that exist in that version

3. **Use compatible version combinations**:

| wasm-bindgen | js-sys    | web-sys   | wasm-bindgen-futures |
|--------------|-----------|-----------|----------------------|
| 0.2.83       | 0.3.60    | 0.3.60    | 0.4.33               |
| 0.2.84       | 0.3.61    | 0.3.61    | 0.4.34               |
| 0.2.87       | 0.3.64    | 0.3.64    | 0.4.37               |
| 0.2.89       | 0.3.66    | 0.3.66    | 0.4.39               |

These combinations are known to work together.

## 2. gRPC and Protocol Buffer Issues

When working with gRPC and Protocol Buffers in WASM, you might encounter additional compatibility issues:

### Solution:

1. **Use matching version combinations** for `tonic` and `prost`:

| tonic       | prost      | tonic-build | tonic-web-wasm-client |
|-------------|------------|-------------|----------------------|
| 0.8.3       | 0.11       | 0.8.4       | 0.3.0                |
| 0.9.2       | 0.11       | 0.9.2       | 0.3.0                |
| 0.10.2      | 0.12       | 0.10.2      | 0.5.0                |

2. **Consider using specific feature flags**:

```toml
tonic = { version = "0.9.2", default-features = false, features = ["codegen", "prost"] }
tonic-build = { version = "0.9.2", default-features = false, features = ["prost"] }
```

## 3. Step-by-Step Approach

If you're still having issues, follow this step-by-step approach:

1. **Start with a minimal version** that doesn't include gRPC or complex features
2. **Get it compiling** with basic functionality
3. **Add features one by one**, checking compilation at each step
4. **Use the feature explorer** to determine available features:

```bash
cargo rustdoc --open -p web-sys -- --document-private-items
```

## 4. Helpful Commands

```bash
# Check for outdated dependencies
cargo outdated

# See the dependency tree to identify version conflicts
cargo tree

# Clean and rebuild to avoid cached artifacts causing issues
cargo clean
wasm-pack clean
wasm-pack build --target web
```

## 5. Last Resort Options

If you continue to face issues:

1. **Use a gRPC-proxy approach**: Write a small Node.js service that consumes the gRPC API and exposes it as a REST API for the WASM module to consume.

2. **Use a simpler transport mechanism**: Consider using WebSockets or HTTP APIs instead of gRPC for browser integration.

3. **Consult the community**: The Rust WebAssembly Working Group has a [Discord channel](https://discord.gg/rust-lang) where you can get help.

Remember that WebAssembly support in Rust continues to improve but can still be challenging for complex use cases. Often, the simplest solution is the most reliable. 