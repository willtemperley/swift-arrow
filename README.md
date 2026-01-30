# Swift Arrow

A Swift implementation of Apache Arrow, the universal columnar format for fast data interchange and in-memory analytics.

The in-memory contiguous buffers allow constant-time random access to large, structured and strongly-typed datasets.

## Project status:

IPC deserialization has been tested against the Arrow integration testing JSON files (gold test files), using the following strategy:

1. Read the Arrow integration test files into memory.
2. Encode the results to Codable & Equatable structs that can read and write the [test data format.](https://arrow.apache.org/docs/format/Integration.html#json-test-data-format).
3. Read the test JSON into the same Codable & Equatable struct and compare with the deserialized results,using Swift equality. This 

IPC serialization uses the same methodology, except a serialization-deserialization round-trip to/from Arrow IPC is performed prior to step 2, i.e. the results under test have been deserialized from IPC, re-serialized to IPC and deserialized again before being compared to the JSON.

The following types are fully supported:

* Primitive types: boolean, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float16, float32, float64.
* Temporal types: timestamp, date32, date64, time32, time64, duration.
* Variable length types: binary and string, plus their fixed-width equivalents
* Nested and recursively nested types: lists and structs, structs of lists etc.
* Maps: Represented as list of key-values, which is spec compliant, however the public API will change.
* Binary views: binaryView and utf8View.

All binary arrays (variable, fixed and view) can be accessed via BinaryArrayProtocol. The same applies to StringArrayProtocol.

## Array interface

Arrow arrays are backed by a standard memory layout:
https://arrow.apache.org/docs/format/Columnar.html

In Swift-Arrow, every array has the following type-erased capabilities:

```swift
public protocol AnyArrowArrayProtocol: Sendable {
  var offset: Int { get }
  var length: Int { get }
  var nullCount: Int { get }
  func slice(offset: Int, length: Int) -> Self
  func any(at index: Int) -> Any?
  var bufferSizes: [Int] { get }
  var buffers: [ArrowBufferProtocol] { get }
}
```

Every array also supports typed access:

```swift
public protocol ArrowArrayProtocol<ItemType>: AnyArrowArrayProtocol {
  associatedtype ItemType
  subscript(_ index: Int) -> ItemType? { get }
}
```

Every Arrow array supports nullable elements. This is encoded as an optional bit-packed validity buffer.
Fixed-width types are encoded back-to-back, with placeholder values for nulls. For example the array:

```swift
let swiftArray: [Int8?] = [1, nil, 2, 3, nil, 4]
let arrayBuilder: ArrayBuilderFixedWidth<Int8> = .init()
for value in swiftArray {
  if let value {
    arrayBuilder.append(value)
  } else {
    arrayBuilder.appendNull()
  }
}
let arrowArray = arrayBuilder.finish()
for i in 0..<swiftArray.count {
  #expect(arrowArray[i] == swiftArray[i])
}
```

would be backed by a values buffer of `Int8`:

`[1, 0, 2, 3, 0, 4]`

and a bit-packed validity buffer of UInt8:
`[45]` or `[b00101101]`

Note the validity buffer may be empty if all values are null, or all values are non null.

Arrow Arrays of variable-length types such as `String` have an offsets buffer. For example:

```swift
let swiftArray: [String?] = ["ab", nil, "c", "", "."]
let arrayBuilder: ArrayBuilderVariable<String> = .init()
for value in swiftArray {
  if let value {
    arrayBuilder.append(value)
  } else {
    arrayBuilder.appendNull()
  }
}
let arrowArray = arrayBuilder.finish()
#expect(arrowArray[0] == "ab")
#expect(arrowArray[1] == nil)
#expect(arrowArray[2] == "c")
#expect(arrowArray[3] == "")
#expect(arrowArray[4] == ".")
```

would have an offsets array of array length + 1 integers:
`[0, 2, 2, 3, 3, 4]`

This is a lookup into the value array, i.e.:

```swift
let values: [UInt8] = [97, 98, 99, 46]
print(values[0..<2]) // [97, 98]
print(values[2..<2]) // []
print(values[2..<3]) // [99]
print(values[3..<4]) // [46]
```

In practice, buffers can be any contingous storage. In Swift-Arrow, arrays created in memory are usually backed by pointers, whereas arrays loaded from IPC files are backed by memory-mapped `Data` instances.

Arrays can be configured to use different buffer types, by specifying the types as 
`public struct ArrowArrayVariable<OffsetsBuffer, ValueBuffer>`

this allows the buffer types to be user-specified, e.g.:
```
typealias ArrowArrayUtf8 = ArrowArrayVariable<
  FixedWidthBufferIPC<Int32>,
  VariableLengthBufferIPC<String>
>
```

## Relationship to Arrow-Swift

This project is based on Arrow-Swift, the official Swift implementation of Apache Arrow. The decision was made to at least temporarily operate independently of the Apache Software Foundation (ASF) to improve development velocity.

The intention is to continue contributing to the official Apache-Swift repository, however changes can be iterated on more quickly here.

Original source: https://github.com/apache/arrow-swift

Changes made since forking Arrow-Swift:
* `ArrowType` has been moved from a class hierarchy to an enum to improve usability and concurrency support.
* IPC is now fully zero-copy, whereas previously file data were copied to pointer-backed arrays.
* Gold-standard IPC tests have been added.
* CI uses the swiftlang workflows: https://github.com/swiftlang/github-workflows
* Tests have been migrated to Swift Testing.
* A DockerFile for compiling ArrowFlight protocol buffers and grpc classes is provided.
* C import/export has been removed.

