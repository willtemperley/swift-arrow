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

/// The type-independent Arrow array capabilities.
public protocol AnyArrowArrayProtocol: Sendable {
  var offset: Int { get }
  var length: Int { get }
  var nullCount: Int { get }
  func slice(offset: Int, length: Int) -> Self
  func any(at index: Int) -> Any?
  var bufferSizes: [Int] { get }
  var buffers: [ArrowBufferProtocol] { get }
}

/// Typed array conformance.
///
/// Public access to typed arays is provided via concrete types or individual protocols as appropriate.
public protocol ArrowArrayProtocol<ItemType>: AnyArrowArrayProtocol {
  associatedtype ItemType
  subscript(_ index: Int) -> ItemType? { get }
}

// This exists to support type-erased struct arrays.
extension ArrowArrayProtocol {
  public func any(at index: Int) -> Any? {
    self[index] as Any?
  }
}

// MARK: Capability protocols.

/// A type which provides access to arrays of utf8 encoded `String`,  with opaque offset types.
///
/// The underlying array may be `String` or `LargeString`.
public protocol StringArrayProtocol: AnyArrowArrayProtocol {
  subscript(index: Int) -> String? { get }
}
extension ArrowArrayVariable: StringArrayProtocol where ItemType == String {}
extension ArrowArrayBinaryView: StringArrayProtocol where ItemType == String {}

/// A type which provides access to arrays of `Data`,  with opaque offset types.
///
/// The underlying array may have fixed or variable-length items.
public protocol BinaryArrayProtocol: AnyArrowArrayProtocol {
  subscript(index: Int) -> Data? { get }
}
extension ArrowArrayFixedSizeBinary: BinaryArrayProtocol {}
extension ArrowArrayVariable: BinaryArrayProtocol
where ItemType == Data, OffsetType: FixedWidthInteger & SignedInteger {}
extension ArrowArrayBinaryView: BinaryArrayProtocol where ItemType == Data {}

public protocol ListArrayProtocol: AnyArrowArrayProtocol {
  var values: AnyArrowArrayProtocol { get }
  subscript(index: Int) -> AnyArrowArrayProtocol? { get }
}
extension ArrowListArray: ListArrayProtocol {}
extension ArrowFixedSizeListArray: ListArrayProtocol {}
// TODO: Add large lists.

// MARK: Array implementations.

/// An Arrow array of booleans using the three-valued logical model (true / false / null).
public struct ArrowArrayBoolean: ArrowArrayProtocol {
  public typealias ItemType = Bool
  public let offset: Int
  public let length: Int
  public var bufferSizes: [Int] { [nullBuffer.length, valueBuffer.length] }
  public var buffers: [ArrowBufferProtocol] { [nullBuffer, valueBuffer] }
  public var nullCount: Int { nullBuffer.nullCount }
  let nullBuffer: NullBuffer
  let valueBuffer: NullBuffer

  public init(
    offset: Int,
    length: Int,
    nullBuffer: NullBuffer,
    valueBuffer: NullBuffer
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.valueBuffer = valueBuffer
  }

  public subscript(index: Int) -> Bool? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    return valueBuffer.isSet(offsetIndex)
  }

  public func slice(offset: Int, length: Int) -> ArrowArrayBoolean {
    .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// An Arrow array of fixed-width types. Uses existential types which are much slower.
public struct ArrowArrayNumericExistential<ItemType: Numeric & BitwiseCopyable>:
  ArrowArrayProtocol
{
  public let offset: Int
  public let length: Int
  public var nullCount: Int { nullBuffer.nullCount }
  public var bufferSizes: [Int] { [nullBuffer.length, valueBuffer.length] }
  public var buffers: [ArrowBufferProtocol] { [nullBuffer, valueBuffer] }

  let nullBuffer: NullBuffer
  private let valueBuffer: any FixedWidthBufferProtocol<ItemType>

  // Initialize from concrete buffer type
  public init<ValueBuffer: FixedWidthBufferProtocol>(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    valueBuffer: ValueBuffer
  ) where ValueBuffer.ElementType == ItemType {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.valueBuffer = valueBuffer
  }

  public subscript(index: Int) -> ItemType? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    return valueBuffer[offsetIndex]
  }

  public func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// An Arrow array of fixed-width types.
public struct ArrowArrayNumeric<ItemType: Numeric & BitwiseCopyable>:
  ArrowArrayProtocol
{
  public let offset: Int
  public let length: Int
  public var nullCount: Int { nullBuffer.nullCount }
  public var bufferSizes: [Int] { [nullBuffer.length, valueBuffer.length] }
  public var buffers: [ArrowBufferProtocol] { [nullBuffer, valueBuffer] }

  let nullBuffer: NullBuffer
  private let valueBuffer: FixedWidthBufferStorage<ItemType>

  // Initialize from concrete buffer type
  public init(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    valueBuffer: FixedWidthBufferStorage<ItemType>
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.valueBuffer = valueBuffer
  }

  public subscript(index: Int) -> ItemType? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    return valueBuffer[offsetIndex]
  }

  public func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

public struct ArrowArrayFixedSizeBinary: ArrowArrayProtocol {
  public typealias ItemType = Data
  public let offset: Int
  public let length: Int
  public let byteWidth: Int

  public var bufferSizes: [Int] { [nullBuffer.length, valueBuffer.length] }
  public var buffers: [ArrowBufferProtocol] { [nullBuffer, valueBuffer] }

  public var nullCount: Int { nullBuffer.nullCount }

  let nullBuffer: NullBuffer
  let valueBuffer: any VariableLengthBufferProtocol<Data>

  public init(
    offset: Int = 0,
    length: Int,
    byteWidth: Int,
    nullBuffer: NullBuffer,
    valueBuffer: any VariableLengthBufferProtocol<Data>
  ) {
    self.offset = offset
    self.length = length
    self.byteWidth = byteWidth
    self.nullBuffer = nullBuffer
    self.valueBuffer = valueBuffer
  }

  public subscript(index: Int) -> ItemType? {
    guard nullBuffer.isSet(index) else { return nil }
    let startIndex = index * byteWidth
    return valueBuffer.loadVariable(at: startIndex, arrayLength: byteWidth)
  }

  public func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: self.offset + offset,  // relative to current offset
      length: length,
      byteWidth: byteWidth,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// An Arrow array of variable-length types.
public struct ArrowArrayVariable<
  ItemType: VariableLength,
  OffsetType: FixedWidthInteger & SignedInteger
>: ArrowArrayProtocol {
  public let offset: Int
  public let length: Int
  private let nullBuffer: NullBuffer
  private let offsetsBuffer: any FixedWidthBufferProtocol<OffsetType>
  private let valueBuffer: any VariableLengthBufferProtocol<ItemType>

  public var bufferSizes: [Int] {
    [nullBuffer.length, offsetsBuffer.length, valueBuffer.length]
  }

  public var buffers: [ArrowBufferProtocol] {
    [nullBuffer, offsetsBuffer, valueBuffer]
  }

  public var nullCount: Int { nullBuffer.nullCount }

  public init<
    Offsets: FixedWidthBufferProtocol<OffsetType>,
    Values: VariableLengthBufferProtocol
  >(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    offsetsBuffer: Offsets,
    valueBuffer: Values
  ) where Values.ElementType == ItemType {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.offsetsBuffer = offsetsBuffer
    self.valueBuffer = valueBuffer
  }

  public subscript(index: Int) -> ItemType? {
    let offsetIndex = self.offset + index
    guard self.nullBuffer.isSet(offsetIndex) else {
      return nil
    }

    // Use runtime dispatch through the existential
    let startOffset = offsetsBuffer[offsetIndex]
    let endOffset = offsetsBuffer[offsetIndex + 1]

    precondition(endOffset >= startOffset, "Corrupted Arrow data")
    return valueBuffer.loadVariable(
      at: Int(startOffset),
      arrayLength: Int(endOffset - startOffset)
    )
  }

  public func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// An Arrow array of `Date`s with a resolution of 1 day.
public struct ArrowArrayDate32: ArrowArrayProtocol {
  public typealias ItemType = Date
  public var bufferSizes: [Int] { array.bufferSizes }
  public var buffers: [ArrowBufferProtocol] { array.buffers }
  public var nullCount: Int { array.nullCount }
  public var offset: Int { array.offset }
  public var length: Int { array.length }
  let array: ArrowArrayNumeric<Date32>

  public subscript(index: Int) -> Date? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    let days: Int32? = array[offsetIndex]
    if let days {
      return Date(timeIntervalSince1970: TimeInterval(days * 86400))
    } else {
      return nil
    }
  }

  public func slice(offset: Int, length: Int) -> Self {
    let internalSlice = array.slice(offset: offset, length: length)
    return .init(array: internalSlice)
  }
}

/// An Arrow array of `Date`s with a resolution of 1 second.
public struct ArrowArrayDate64: ArrowArrayProtocol {
  public typealias ItemType = Date
  public var bufferSizes: [Int] { array.bufferSizes }
  public var buffers: [ArrowBufferProtocol] { array.buffers }
  public var nullCount: Int { array.nullCount }
  public var offset: Int { array.offset }
  public var length: Int { array.length }
  let array: ArrowArrayNumeric<Date64>

  public subscript(index: Int) -> Date? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    let milliseconds: Int64? = array[offsetIndex]
    if let milliseconds {
      return Date(timeIntervalSince1970: TimeInterval(milliseconds / 1000))
    } else {
      return nil
    }
  }

  public func slice(offset: Int, length: Int) -> Self {
    let internalSlice = array.slice(offset: offset, length: length)
    return .init(array: internalSlice)
  }
}

///// An Arrow list array which may be nested arbitrarily.
public struct ArrowListArray<
  OffsetType: FixedWidthInteger & SignedInteger
>: ArrowArrayProtocol {
  public let offset: Int
  public let length: Int
  public var bufferSizes: [Int] {
    [nullBuffer.length, offsetsBuffer.length]
  }
  public var buffers: [ArrowBufferProtocol] {
    [nullBuffer, offsetsBuffer]
  }
  public var nullCount: Int { nullBuffer.nullCount }

  let nullBuffer: NullBuffer
  let offsetsBuffer: any FixedWidthBufferProtocol<OffsetType>
  public let values: AnyArrowArrayProtocol

  public init(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    offsetsBuffer: any FixedWidthBufferProtocol<OffsetType>,
    values: AnyArrowArrayProtocol
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.offsetsBuffer = offsetsBuffer
    self.values = values
  }

  public subscript(index: Int) -> AnyArrowArrayProtocol? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    let startIndex = offsetsBuffer[offsetIndex]
    let endIndex = offsetsBuffer[offsetIndex + 1]
    let length = endIndex - startIndex
    return values.slice(offset: Int(startIndex), length: Int(length))
  }

  public func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: self.offset + offset,
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      values: values
    )
  }
}

/// An Arrow list array with fixed size elements.
public struct ArrowFixedSizeListArray: ArrowArrayProtocol {
  public let offset: Int
  public let length: Int
  public let listSize: Int

  public var bufferSizes: [Int] {
    [nullBuffer.length]
  }

  public var buffers: [ArrowBufferProtocol] {
    [nullBuffer]
  }

  public var nullCount: Int { nullBuffer.nullCount }

  let nullBuffer: NullBuffer
  public let values: AnyArrowArrayProtocol

  public init(
    offset: Int = 0,
    length: Int,
    listSize: Int,
    nullBuffer: NullBuffer,
    values: AnyArrowArrayProtocol
  ) {
    self.offset = offset
    self.length = length
    self.listSize = listSize
    self.nullBuffer = nullBuffer
    self.values = values
  }

  public subscript(index: Int) -> AnyArrowArrayProtocol? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index

    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }

    let startIndex = offsetIndex * listSize
    return values.slice(offset: startIndex, length: listSize)
  }

  public func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: self.offset + offset,
      length: length,
      listSize: listSize,
      nullBuffer: nullBuffer,
      values: values
    )
  }
}

/// An Arrow struct array.
public struct ArrowStructArray: ArrowArrayProtocol {
  public typealias ItemType = [String: Any]
  public let offset: Int
  public let length: Int
  public let fields: [(name: String, array: AnyArrowArrayProtocol)]
  public var bufferSizes: [Int] { [nullBuffer.length] }
  public var buffers: [ArrowBufferProtocol] { [nullBuffer] }
  public var nullCount: Int { nullBuffer.nullCount }
  let nullBuffer: NullBuffer

  public init(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    fields: [(name: String, array: AnyArrowArrayProtocol)]
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.fields = fields
  }

  public subscript(index: Int) -> ItemType? {
    guard nullBuffer.isSet(offset + index) else { return nil }
    var result: [String: Any] = [:]
    for (name, array) in fields {
      result[name] = array.any(at: index)
    }
    return result
  }

  public func slice(offset newOffset: Int, length newLength: Int) -> Self {
    .init(
      offset: self.offset + newOffset,
      length: newLength,
      nullBuffer: nullBuffer,
      fields: fields
    )
  }
}
