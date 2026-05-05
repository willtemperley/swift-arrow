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

/// Builds Arrow arrays of variable length types such as `String`.
final class VariableLengthTypeBufferBuilder<T> where T: VariableLength {
  var length: Int
  var capacity: Int
  private var buffer: UnsafeMutablePointer<UInt8>
  private var ownsMemory: Bool

  func getBuffer() -> UnsafeMutablePointer<UInt8> {
    self.buffer
  }

  init(
    minCapacity: Int = 64
  ) {
    self.length = 0
    self.capacity = minCapacity
    self.buffer = .allocate(capacity: capacity)
    self.ownsMemory = true
  }

  func append(_ data: Data) {

    let buffer = getBuffer()

    data.withUnsafeBytes { dataBuffer in
      UnsafeMutableRawBufferPointer(
        start: buffer.advanced(by: length),
        count: data.count
      ).copyBytes(from: dataBuffer)
    }

    length += data.count
  }

  func increaseCapacity(to newCapacity: Int) {
    resize(to: newCapacity)
  }

  private func resize(to newCapacity: Int) {
    precondition(newCapacity > capacity)
    let newBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: newCapacity)
    newBuffer.initialize(from: buffer, count: length)
    buffer.deallocate()
    buffer = newBuffer
    capacity = newCapacity
  }

  deinit {
    if ownsMemory {
      buffer.deallocate()
    }
  }

  /// Builds completed `VariableBuffer` with 64-byte alignment.
  ///
  /// Memory ownership is transferred to the returned `VariableBuffer`.  Any memory held is
  /// deallocated.
  /// - Returns: the completed `NullBuffer` with capacity shrunk to a multiple of 64 bytes.
  func finish() -> VariableLengthBuffer<T> {
    precondition(ownsMemory, "Buffer already finished.")
    ownsMemory = false
    let newCapacity = (length + 63) & ~63
    let newBuffer = UnsafeMutableRawPointer.allocate(
      byteCount: newCapacity,
      alignment: 64
    ).bindMemory(to: UInt8.self, capacity: newCapacity)
    newBuffer.initialize(from: buffer, count: length)
    buffer.deallocate()
    return VariableLengthBuffer(
      length: length,
      capacity: newCapacity,
      ownsMemory: true,
      buffer: newBuffer
    )
  }
}
