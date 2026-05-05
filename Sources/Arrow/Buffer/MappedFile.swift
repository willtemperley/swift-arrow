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

import BinaryParsing
import Foundation

public final class MappedFile: @unchecked Sendable {
  let pointer: UnsafeRawPointer
  let size: Int
  private let fd: Int32

  public init(path: String) throws {
    self.fd = open(path, O_RDONLY)
    guard fd >= 0 else { throw POSIXError(.ENOENT) }

    var stat = stat()
    fstat(fd, &stat)
    self.size = Int(stat.st_size)

    let mapped = mmap(nil, size, PROT_READ, MAP_PRIVATE, fd, 0)
    guard mapped != MAP_FAILED else {
      close(fd)
      throw POSIXError(.ENOMEM)
    }
    self.pointer = UnsafeRawPointer(mapped!)
  }

  deinit {
    munmap(UnsafeMutableRawPointer(mutating: pointer), size)
    close(fd)
  }
}

extension MappedFile: ParserSpanProvider {
  public func withParserSpan<T, E>(
    _ body: (inout ParserSpan) throws(E) -> T
  ) throws(E) -> T {
    let buffer = UnsafeRawBufferPointer(start: pointer, count: size)
    var span = unsafe ParserSpan(_unsafeBytes: buffer)
    return try body(&span)
  }
}
