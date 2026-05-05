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
  var buffer: FileDataBuffer { get }
}

extension ArrowBufferIPC {
  public func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R {
    try buffer.data.withUnsafeBytes { dataPtr in
      let rangedPtr = UnsafeRawBufferPointer(
        rebasing: dataPtr[buffer.range]
      )
      return try body(rangedPtr)
    }
  }
}

/// A `Data` backed buffer for null bitmaps and boolean arrays.
public struct NullBufferIPC: NullBuffer, ArrowBufferIPC {

  let buffer: FileDataBuffer
  public var valueCount: Int
  public let nullCount: Int

  public var length: Int { (valueCount + 7) / 8 }

  public init(buffer: FileDataBuffer, valueCount: Int, nullCount: Int) {
    self.buffer = buffer
    self.valueCount = valueCount
    self.nullCount = nullCount
  }

  public func isSet(_ bit: Int) -> Bool {
    precondition(bit < valueCount, "Bit index \(bit) out of range")
    let byteIndex = bit / 8
    let offsetIndex = buffer.range.lowerBound + byteIndex
    let byte = self.buffer.data[offsetIndex]
    return byte & (1 << (bit % 8)) > 0
  }
}

/// A `Data` backed buffer for fixed-width types.
public struct FixedWidthBufferIPC<Element>: FixedWidthBufferProtocol,
  ArrowBufferIPC
where Element: BitwiseCopyable {
  public typealias ElementType = Element
  let buffer: FileDataBuffer
  public var length: Int { buffer.range.count }

  public init(buffer: FileDataBuffer) {
    self.buffer = buffer
  }

  public subscript(index: Int) -> Element {
    buffer.data.withUnsafeBytes { rawBuffer in
      let sub = rawBuffer[buffer.range]
      let span = Span<Element>(_unsafeBytes: sub)
      return span[index]
    }
  }
}

public struct FixedWidthBufferIPC2<Element: BitwiseCopyable>:
  @unchecked Sendable
{
  let buffer: FileDataBuffer2

  public init(buffer: FileDataBuffer2) {
    self.buffer = buffer
  }

  public subscript(index: Int) -> Element {
    buffer.basePointer.load(
      fromByteOffset: index * MemoryLayout<Element>.stride,
      as: Element.self
    )
  }
}

/// A `Data` backed buffer for variable-length types.
public struct VariableLengthBufferIPC<
  Element: VariableLength, OffsetType: FixedWidthInteger
>:
  VariableLengthBufferProtocol, ArrowBufferIPC
{
  public typealias ElementType = Element
  let buffer: FileDataBuffer
  public var length: Int { buffer.range.count }

  public init(buffer: FileDataBuffer) {
    self.buffer = buffer
  }

  public func loadVariable(
    at startIndex: Int,
    arrayLength: Int
  ) -> Element {
    precondition(startIndex + arrayLength <= self.length)
    return buffer.data.withUnsafeBytes { rawBuffer in
      let offsetStart = buffer.range.lowerBound + startIndex
      let offsetEnd = offsetStart + arrayLength
      let slice = rawBuffer[offsetStart..<offsetEnd]
      let uint8Buffer = slice.bindMemory(to: UInt8.self)
      return Element(uint8Buffer)
    }
  }
}
