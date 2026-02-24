// swift-tools-version: 6.2

// Copyright 2025 The Apache Software Foundation
// Copyright 2025 The swift-arrow contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import PackageDescription

let package = Package(
  name: "Arrow",
  platforms: [
    .macOS(.v26), .iOS(.v26), .watchOS(.v26), .tvOS(.v26), .visionOS(.v26),
  ],
  products: [
    .library(name: "Arrow", targets: ["Arrow"]),
    .library(name: "ArrowIPC", targets: ["ArrowIPC"]),
  ],
  dependencies: [
    .package(
      url: "https://github.com/google/flatbuffers.git",
      exact: "25.12.19"
    ),
    .package(
      url: "https://github.com/apple/swift-atomics.git",
      from: "1.3.0"
    ),
    .package(
      url: "https://github.com/grpc/grpc-swift.git",
      from: "1.25.0"
    ),
    .package(
      url: "https://github.com/apple/swift-protobuf.git",
      from: "1.29.0"
    ),
    .package(
      url: "https://github.com/apple/swift-binary-parsing.git",
      from: "0.0.1"
    ),
    .package(
      url: "https://github.com/swiftlang/swift-subprocess.git",
      branch: "main"
    ),
  ],
  targets: [
    .target(
      name: "ArrowC",
      swiftSettings: [
        // build: .unsafeFlags(["-warnings-as-errors"])
      ]
    ),
    .target(
      name: "Arrow",
      // TODO: clean up after removing old backend
      dependencies: [
        "ArrowC",
        .product(name: "FlatBuffers", package: "flatbuffers"),
        .product(name: "Atomics", package: "swift-atomics"),
      ],
      swiftSettings: [
        // build: .unsafeFlags(["-warnings-as-errors"])
      ]
    ),
    .target(
      name: "ArrowIPC",
      dependencies: [
        "Arrow",
        "ArrowC",
        .product(name: "FlatBuffers", package: "flatbuffers"),
        .product(name: "BinaryParsing", package: "swift-binary-parsing"),
      ],
      swiftSettings: [
        // build: .unsafeFlags(["-warnings-as-errors"])
      ]
    ),
    .target(
      name: "ArrowFlight",
      dependencies: [
        "Arrow",
        "ArrowIPC",
        .product(name: "GRPC", package: "grpc-swift"),
        .product(name: "SwiftProtobuf", package: "swift-protobuf"),
      ],
      swiftSettings: [
        // build: .unsafeFlags(["-warnings-as-errors"])
      ]
    ),
    .testTarget(
      name: "ArrowTests",
      dependencies: ["Arrow", "ArrowC"],
      // TODO: clean up after removing old backend
      resources: [
        .copy("Resources/")
      ],
      swiftSettings: [
        // build: .unsafeFlags(["-warnings-as-errors"])
      ]
    ),
    .testTarget(
      name: "ArrowIPCTests",
      dependencies: [
        "Arrow",
        "ArrowIPC",
        .product(name: "Subprocess", package: "swift-subprocess"),
      ],
      resources: [
        .copy("Resources/")
      ],
      swiftSettings: [
        // build: .unsafeFlags(["-warnings-as-errors"])
      ]
    ),
    .testTarget(
      name: "ArrowFlightTests",
      dependencies: [
        "Arrow",
        "ArrowFlight",
        .product(name: "GRPC", package: "grpc-swift"),
        .product(name: "SwiftProtobuf", package: "swift-protobuf"),
      ],
      swiftSettings: [
        // build: .unsafeFlags(["-warnings-as-errors"])
      ]
    ),
  ]
)
