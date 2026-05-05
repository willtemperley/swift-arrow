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

/// A type used to represent nulls and booleans in Arrow arrays.
public protocol NullBuffer: ArrowBufferProtocol {
  var length: Int { get }
  var valueCount: Int { get }
  var nullCount: Int { get }
  func isSet(_ bit: Int) -> Bool
}

/// Represents an array with no nulls (all values valid).
public struct AllValidNullBuffer: NullBuffer, ArrowBufferEmpty {
  public let valueCount: Int
  public var length: Int { 0 }
  public var nullCount: Int { 0 }

  public init(valueCount: Int) {
    self.valueCount = valueCount
  }

  @inlinable
  public func isSet(_ bit: Int) -> Bool {
    precondition(bit < valueCount)
    return true
  }
}

/// Represents an array with all nulls.
public struct AllNullBuffer: NullBuffer, ArrowBufferEmpty {
  public let valueCount: Int
  public var length: Int { 0 }
  public var nullCount: Int { valueCount }

  public init(valueCount: Int) {
    self.valueCount = valueCount
  }

  public func isSet(_ bit: Int) -> Bool {
    precondition(bit < valueCount)
    return false
  }
}

/// A  bit-packed buffer used to represent nulls and booleans in Arrow arrays.
final class BitPackedNullBuffer: NullBuffer, ArrowBufferUInt8,
  @unchecked Sendable
{
  let length: Int
  let capacity: Int
  let valueCount: Int
  let ownsMemory: Bool
  let buffer: UnsafePointer<UInt8>
  let nullCount: Int

  init(
    length: Int,
    capacity: Int,
    valueCount: Int,
    nullCount: Int,
    ownsMemory: Bool,
    buffer: UnsafePointer<UInt8>
  ) {
    self.length = length
    self.capacity = capacity
    self.valueCount = valueCount
    self.nullCount = nullCount
    self.ownsMemory = ownsMemory
    self.buffer = buffer
  }

  func isSet(_ bit: Int) -> Bool {
    let byteIndex = bit / 8
    precondition(length > byteIndex, "Bit index \(bit) out of range")
    let byte = self.buffer[byteIndex]
    return byte & (1 << (bit % 8)) > 0
  }

  deinit {
    if ownsMemory {
      buffer.deallocate()
    }
  }
}
