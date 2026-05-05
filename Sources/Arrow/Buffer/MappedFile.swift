// MappedFile.swift
// Arrow
//
// Created by Will Temperley on 05/05/2026. All rights reserved.
// Copyright 2026 Will Temperley.
//
// Copying or reproduction of this file via any medium requires prior express
// written permission from the copyright holder.
// -----------------------------------------------------------------------------
///
/// Implementation notes, links and internal documentation go here.
///
// -----------------------------------------------------------------------------

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
