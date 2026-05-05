// Copyright 2025 The Columnar Swift Contributors
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

import Foundation

public protocol AnyArrayBuilder {
  associatedtype ArrayType: ArrowArrayProtocol
  var length: Int { get }
  func appendNull()
  func append(_ value: ArrayType.ItemType)
  func appendAny(_ value: Any)
  func finish() -> ArrayType
}

extension AnyArrayBuilder {
  public func appendAny(_ value: Any) {
    guard let x = value as? ArrayType.ItemType else {
      fatalError(
        "Type mismatch: \(type(of: self)) expects \(ArrayType.self), got \(type(of: value))"
      )
    }
    append(x)
  }
}

/// A builder for Arrow arrays using the three-valued logical model (true / false / null).
public class ArrayBuilderBoolean: AnyArrayBuilder {

  public typealias ArrayType = ArrowArrayBoolean

  public var length: Int
  let nullBuilder: NullBufferBuilder
  let valueBuilder: NullBufferBuilder

  public init() {
    self.length = 0
    self.nullBuilder = NullBufferBuilder()
    self.valueBuilder = NullBufferBuilder()
  }

  public func append(_ value: Bool) {
    length += 1
    nullBuilder.appendValid(true)
    valueBuilder.appendValid(value)
  }

  public func appendNull() {
    length += 1
    nullBuilder.appendValid(false)
    valueBuilder.appendValid(false)
  }

  public func finish() -> ArrayType {
    let nullBuffer = nullBuilder.finish()
    let valueBuffer = valueBuilder.finish()

    return .init(
      offset: 0,
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// A builder for Arrow arrays holding fixed-width types.
public class ArrayBuilderNumeric<T: Numeric & BitwiseCopyable>:
  AnyArrayBuilder
{

  public typealias ArrayType = ArrowArrayNumeric<T>

  public var length: Int
  let nullBuilder: NullBufferBuilder
  let valueBuilder: FixedWidthBufferBuilder<T>

  public init() {
    self.length = 0
    self.nullBuilder = NullBufferBuilder()
    self.valueBuilder = FixedWidthBufferBuilder<T>()
  }

  public func append(_ value: T) {
    length += 1
    nullBuilder.appendValid(true)
    valueBuilder.append(value)
  }

  public func appendNull() {
    length += 1
    nullBuilder.appendValid(false)
    valueBuilder.append(T.zero)
  }

  public func finish() -> ArrayType {
    let nullBuffer = nullBuilder.finish()
    let valueBuffer = valueBuilder.finish()

    return .init(
      offset: 0,
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: .allocated(valueBuffer)
    )
  }
}

/// A builder for Arrow arrays holding variable length types.
public class ArrayBuilderFixedSizedBinary:
  AnyArrayBuilder
{
  public typealias ArrayType = ArrowArrayFixedSizeBinary

  public var length: Int
  let byteWidth: Int
  let nullBuilder: NullBufferBuilder
  let valueBuilder: VariableLengthTypeBufferBuilder<Data>
  let nullValue: Data

  public init(byteWidth: Int) {
    self.length = 0
    self.byteWidth = byteWidth
    self.nullBuilder = NullBufferBuilder()
    self.valueBuilder = VariableLengthTypeBufferBuilder<Data>()
    self.nullValue = Data(repeating: 0, count: byteWidth)
  }

  public func append(_ value: Data) {
    length += 1
    nullBuilder.appendValid(true)
    precondition(value.count == byteWidth, "Incorrect byte width.")
    let requiredCapacity = valueBuilder.length + value.count
    if requiredCapacity > valueBuilder.capacity {
      var newCapacity = valueBuilder.capacity
      while newCapacity < requiredCapacity {
        newCapacity *= 2
      }
      valueBuilder.increaseCapacity(to: newCapacity)
    }
    valueBuilder.append(value)
  }

  public func appendNull() {
    length += 1
    nullBuilder.appendValid(false)
    valueBuilder.append(nullValue)
  }

  public func finish() -> ArrayType {
    let nullBuffer = nullBuilder.finish()
    let valueBuffer = valueBuilder.finish()
    return .init(
      offset: 0,
      length: length,
      byteWidth: byteWidth,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// A builder for Arrow arrays holding variable length types.
public class ArrayBuilderVariableLength<
  Element: VariableLength,
  OffsetType: FixedWidthInteger & SignedInteger
>: AnyArrayBuilder {

  public typealias ArrayType = ArrowArrayVariable<Element, OffsetType>

  public var length: Int
  let nullBuilder: NullBufferBuilder
  let offsetsBuilder: FixedWidthBufferBuilder<OffsetType>
  let valueBuilder: VariableLengthTypeBufferBuilder<Element>

  public init() {
    self.length = 0
    self.nullBuilder = NullBufferBuilder()
    self.offsetsBuilder = FixedWidthBufferBuilder<OffsetType>()
    self.valueBuilder = VariableLengthTypeBufferBuilder<Element>()
    self.offsetsBuilder.append(OffsetType.zero)
  }

  public func append(_ value: Element) {
    length += 1
    nullBuilder.appendValid(true)
    let data = value.data
    let requiredCapacity = valueBuilder.length + data.count
    if requiredCapacity > valueBuilder.capacity {
      var newCapacity = valueBuilder.capacity
      while newCapacity < requiredCapacity {
        newCapacity *= 2
      }
      valueBuilder.increaseCapacity(to: newCapacity)
    }
    valueBuilder.append(data)
    let newOffset = OffsetType(valueBuilder.length)
    offsetsBuilder.append(newOffset)
  }

  public func appendNull() {
    length += 1
    nullBuilder.appendValid(false)
    let newOffset = OffsetType(valueBuilder.length)
    offsetsBuilder.append(newOffset)
  }

  public func finish() -> ArrayType {
    ArrayType(
      length: length,
      nullBuffer: nullBuilder.finish(),
      offsetsBuffer: offsetsBuilder.finish(),
      valueBuffer: valueBuilder.finish()
    )
  }
}

/// A builder for Arrow arrays holding `String` values.
typealias ArrayBuilderString = ArrayBuilderVariableLength<String, Int32>

/// A builder for Arrow arrays holding `Data` values.
typealias ArrayBuilderBinary = ArrayBuilderVariableLength<Data, Int32>

/// A builder for Arrow arrays holding `Date`s with a resolution of one day.
public struct ArrayBuilderDate32: AnyArrayBuilder {
  public typealias ArrayType = ArrowArrayDate32
  let builder: ArrayBuilderNumeric<Date32> = .init()

  public init() {}

  public var length: Int {
    builder.length
  }

  public func append(_ value: Date) {
    let daysSinceEpoch = Int32(value.timeIntervalSince1970 / 86400)
    self.builder.append(daysSinceEpoch)
  }

  public func appendNull() {
    builder.appendNull()
  }

  public func finish() -> ArrayType {
    .init(array: builder.finish())
  }
}

/// A builder for Arrow arrays holding `Date`s with a resolution of one day.
public struct ArrayBuilderDate64: AnyArrayBuilder {
  public typealias ArrayType = ArrowArrayDate64
  let builder: ArrayBuilderNumeric<Date64> = .init()

  public init() {}

  public var length: Int {
    builder.length
  }

  public func appendNull() {
    self.builder.appendNull()
  }

  public func append(_ value: Date) {
    let millisecondsSinceEpoch = Int64(value.timeIntervalSince1970 * 1000)
    self.builder.append(millisecondsSinceEpoch)
  }

  public func finish() -> ArrayType {
    .init(array: builder.finish())
  }
}

/// A builder for Arrow arrays holding Time32 values.
public typealias ArrayBuilderTime32 = ArrayBuilderNumeric<Time32>

/// A builder for Arrow arrays holding Time64 values.
public typealias ArrayBuilderTime64 = ArrayBuilderNumeric<Time64>

/// A builder for Arrow arrays holding Timestamp values.
public typealias ArrayBuilderTimestamp = ArrayBuilderNumeric<Timestamp>

public class ArrayBuilderList<T: AnyArrayBuilder> {

  typealias ArrayType = ArrowListArray<Int32>

  var length: Int
  let nullBuilder: NullBufferBuilder
  let offsetsBuilder: FixedWidthBufferBuilder<Int32>
  let valueBuilder: T  // Child array builder

  init(valueBuilder: T) {
    self.length = 0
    self.nullBuilder = NullBufferBuilder()
    self.offsetsBuilder = FixedWidthBufferBuilder<Int32>()
    self.valueBuilder = valueBuilder
    self.offsetsBuilder.append(Int32.zero)
  }

  // Append a list by providing a closure that populates the child builder
  func append(_ builder: (T) -> Void) {
    length += 1
    nullBuilder.appendValid(true)

    builder(valueBuilder)  // User adds items to child builder
    let endLength = valueBuilder.length

    offsetsBuilder.append(Int32(endLength))
  }

  func appendNull() {
    length += 1
    nullBuilder.appendValid(false)
    // Append current child length (empty slice)
    offsetsBuilder.append(Int32(valueBuilder.length))
  }

  func finish() -> ArrayType {
    let nullBuffer = nullBuilder.finish()
    let offsetsBuffer = offsetsBuilder.finish()
    let valuesArray = valueBuilder.finish()

    return ArrowListArray(
      offset: 0,
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      values: valuesArray  // Now accepts AnyArrowArrayProtocol
    )
  }
}

class ArrayBuilderStruct: AnyArrayBuilder {

  typealias ArrayType = ArrowStructArray
  var length: Int
  let nullBuilder: NullBufferBuilder
  let fields: [(name: String, builder: any AnyArrayBuilder)]

  init(fields: [(String, any AnyArrayBuilder)]) {
    self.length = 0
    self.nullBuilder = NullBufferBuilder()
    self.fields = fields
  }

  // Append a struct by providing values for each field
  func append(_ value: [String: Any]) {

    length += 1
    nullBuilder.appendValid(true)

    for (name, builder) in fields {
      guard let structValue = value[name] else {
        // Field not provided - append null
        builder.appendNull()
        continue
      }

      builder.appendAny(structValue)
    }
  }

  func appendNull() {
    length += 1
    nullBuilder.appendValid(false)
    // Need to append nulls to all child builders to keep lengths aligned.
    for (_, builder) in fields {
      builder.appendNull()
    }
  }

  func finish() -> ArrowStructArray {
    let nullBuffer = nullBuilder.finish()
    let finishedFields = fields.map { (name, builder) in
      (name: name, array: builder.finish() as any ArrowArrayProtocol)
    }

    return .init(
      offset: 0,
      length: length,
      nullBuffer: nullBuffer,
      fields: finishedFields
    )
  }
}
