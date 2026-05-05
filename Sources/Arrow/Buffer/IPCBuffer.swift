// Copyright 2026 The Columnar Swift Contributors
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

/// An Arrow buffer backed by file data.
internal protocol ArrowBufferIPC: ArrowBufferProtocol {
  var buffer: FileDataBuffer2 { get }
}

extension ArrowBufferIPC {
  public func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R {
    let raw = UnsafeRawBufferPointer(
      start: buffer.basePointer,
      count: buffer.range.count
    )
    return try body(raw)
  }
}

/// A `Data` backed buffer for null bitmaps and boolean arrays.
public struct NullBufferIPC: NullBuffer, ArrowBufferIPC, @unchecked Sendable {
  let buffer: FileDataBuffer2
  public var valueCount: Int
  public let nullCount: Int
  private let base: UnsafePointer<UInt8>

  public var length: Int { (valueCount + 7) / 8 }

  public init(buffer: FileDataBuffer2, valueCount: Int, nullCount: Int) {
    self.buffer = buffer
    self.valueCount = valueCount
    self.nullCount = nullCount
    self.base = buffer.basePointer.assumingMemoryBound(to: UInt8.self)
  }

  public func isSet(_ bit: Int) -> Bool {
    precondition(bit < valueCount, "Bit index \(bit) out of range")
    let byteIndex = bit / 8
    return base[byteIndex] & (1 << (bit % 8)) > 0
  }
}

/// A `Data` backed buffer for fixed-width types.
//public struct FixedWidthBufferIPC<Element>: FixedWidthBufferProtocol,
//  ArrowBufferIPC
//where Element: BitwiseCopyable {
//  public typealias ElementType = Element
//  let buffer: FileDataBuffer
//  public var length: Int { buffer.range.count }
//
//  public init(buffer: FileDataBuffer) {
//    self.buffer = buffer
//  }
//
//  public subscript(index: Int) -> Element {
//    buffer.data.withUnsafeBytes { rawBuffer in
//      let sub = rawBuffer[buffer.range]
//      let span = Span<Element>(_unsafeBytes: sub)
//      return span[index]
//    }
//  }
//}

public struct FixedWidthBufferIPC2<Element: BitwiseCopyable>:
  @unchecked Sendable, FixedWidthBufferProtocol
{
  let buffer: FileDataBuffer2
  let pointer: UnsafePointer<Element>

  public init(buffer: FileDataBuffer2) {
    self.buffer = buffer
    self.pointer = buffer.basePointer.assumingMemoryBound(to: Element.self)
  }

  public subscript(index: Int) -> Element {
    pointer[index]
  }

  public var length: Int { buffer.range.count }

  public func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R {
    let raw = UnsafeRawBufferPointer(
      start: UnsafeRawPointer(pointer),
      count: buffer.range.count
    )
    return try body(raw)
  }
}

/// A `Data` backed buffer for variable-length types.
public struct VariableLengthBufferIPC<
  Element: VariableLength, OffsetType: FixedWidthInteger
>:
  VariableLengthBufferProtocol, ArrowBufferIPC
{
  public typealias ElementType = Element
  let buffer: FileDataBuffer2
  public var length: Int { buffer.range.count }

  public init(buffer: FileDataBuffer2) {
    self.buffer = buffer
  }

  public func loadVariable(
    at startIndex: Int,
    arrayLength: Int
  ) -> Element {
    precondition(startIndex + arrayLength <= self.length)
    let start = buffer.basePointer + startIndex
    let raw = UnsafeRawBufferPointer(start: start, count: arrayLength)
    let uint8Buffer = raw.bindMemory(to: UInt8.self)
    return Element(uint8Buffer)
  }
}
