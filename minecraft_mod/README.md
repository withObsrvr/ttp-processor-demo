# Stellar TTP Minecraft Mod

This Minecraft mod visualizes Token Transfer Protocol (TTP) events from the Stellar blockchain within the Minecraft game environment. It connects to the `ttp-processor` service via gRPC to receive real-time token transfer events and displays them in-game.

## Features

- Real-time visualization of Stellar token transfers, mints, and burns in Minecraft
- In-game commands to control TTP event streaming
- Clean integration with Fabric mod loader
- Connection to the TTP processor service via gRPC

## Prerequisites

Before you begin, ensure you have the following installed:

1. **Java Development Kit (JDK) 17+** - Required to compile the mod
   - Download from [AdoptOpenJDK](https://adoptium.net/) or your preferred provider
   - Verify with `java -version`

2. **Minecraft (with Fabric)**
   - Install [Minecraft Java Edition](https://www.minecraft.net/) (version 1.20.4)
   - Install [Fabric Loader](https://fabricmc.net/use/) for Minecraft

3. **Gradle**
   - The project uses the Gradle wrapper, so you don't need to install Gradle separately

4. **Protocol Buffers Compiler (protoc) 3.25.1+**
   - Download from [Protocol Buffers Releases](https://github.com/protocolbuffers/protobuf/releases)
   - On macOS: `brew install protobuf`
   - On Ubuntu/Debian: `apt install protobuf-compiler`
   - Verify with `protoc --version`

5. **Running TTP Processor Service**
   - The mod requires a running TTP processor service to connect to
   - See the root README for instructions on starting the service

## Building the Mod

1. **Setup Protocol Buffers**

   Run the setup script to prepare the proto files for Java:

   ```bash
   cd minecraft_mod
   chmod +x setup_protos.sh
   ./setup_protos.sh
   ```

   This script:
   - Copies proto files from the `cli_tool` directory
   - Adds Java package options to the proto files
   - Prepares them for gRPC code generation

2. **Build the Mod**

   Use Gradle to build the mod:

   ```bash
   cd minecraft_mod
   ./gradlew build
   ```

   The compiled mod JAR will be created in `build/libs/ttp-mod-1.0.0.jar`.

3. **Install the Mod**

   Copy the generated JAR file to your Minecraft mods folder:

   ```bash
   cp build/libs/ttp-mod-1.0.0.jar ~/.minecraft/mods/
   ```

   Adjust the path according to your Minecraft installation.

## Using the Mod

1. **Start Minecraft with Fabric**

   Launch Minecraft with the Fabric loader. The mod should be loaded automatically.

2. **Configure the Mod**

   By default, the mod connects to `localhost:50052`. To change this:

   - Modify the `DEFAULT_ENDPOINT` in `TTPClient.java` and rebuild the mod, or
   - Future versions may support configuration via a config file

3. **In-Game Commands**

   The mod adds several commands to interact with the TTP stream:

   - Start streaming events:
     ```
     /ttp start <startLedger> <endLedger>
     ```
     Where `<startLedger>` is the ledger to start from and `<endLedger>` is the ledger to end at (use 0 for unbounded streaming).

   - Stop streaming events:
     ```
     /ttp stop
     ```

   - Check streaming status:
     ```
     /ttp status
     ```

4. **Viewing Events**

   Token transfer events are visualized in the 3D world around the player. When an event is received:

   - Visualizations appear randomly within a 40-block radius around the player
   - Each event type has a distinct visual effect:
     - **Transfers**: Blue beams of light shooting upward (up to 20 blocks high)
     - **Mints**: Green particle explosions
     - **Burns**: Red explosion effects
   
   The mod limits the number of simultaneous visual effects to 50 to prevent performance issues. Each effect has a limited lifetime and will automatically fade away after a short period.

   Visualization details:
   - Effects automatically appear as events are received from the TTP processor
   - You don't need to look in any specific direction - the effects will appear around you
   - The effects are visible through walls and other obstacles
   - Text information about the asset, amount, and accounts involved appears alongside the visual effects

## Architecture

The mod consists of several key components:

1. **StellarTTPMod.java** - The main mod class that initializes the mod and registers commands

2. **TTPClient.java** - Handles gRPC communication with the TTP processor service

3. **EventRenderer.java** - Renders TTP events in the Minecraft world

4. **TokenTransferEvent.java** - Internal representation of token transfer events

5. **StellarTTPModClient.java** - Client-side initialization and rendering

## Development

### Directory Structure

```
minecraft_mod/
├── src/
│   ├── main/
│   │   ├── java/com/stellar/ttpmod/
│   │   │   ├── client/
│   │   │   ├── event/
│   │   │   ├── grpc/
│   │   │   ├── render/
│   │   │   ├── StellarTTPMod.java
│   │   ├── proto/
│   │   │   ├── event_service/
│   │   │   ├── ingest/
│   │   ├── resources/
│   │   │   ├── assets/
│   │   │   ├── fabric.mod.json
├── build.gradle
├── gradle.properties
├── setup_protos.sh
```

### Customizing the Mod

To customize the mod:

1. **Modify Event Visualization**
   - Edit `EventRenderer.java` to change how events are displayed in-game

2. **Add New Commands**
   - Extend the command registration in `StellarTTPMod.java`

3. **Change gRPC Endpoint**
   - Modify `DEFAULT_ENDPOINT` in `TTPClient.java`

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Check that the TTP processor service is running
   - Verify the endpoint configuration

2. **Missing Dependencies**
   - Ensure you have installed Fabric API in your mods folder

3. **Protocol Buffer Errors**
   - Run `setup_protos.sh` again to refresh the proto files
   - Check that protoc is installed correctly

4. **Build Failures**
   - Ensure JDK 17+ is installed and set as the default
   - Check Gradle logs for specific errors

## License

This mod is licensed under the Apache License 2.0. See the LICENSE file for details. 