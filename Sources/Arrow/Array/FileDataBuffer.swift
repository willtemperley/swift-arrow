// FileDataBuffer.swift
// Arrow
//
// Created by Will Temperley on 04/05/2026. All rights reserved.
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

/// A view over `MappedFile` which backs an Arrow buffer.
public struct FileDataBuffer: Sendable {
  let file: MappedFile
  let range: Range<Int>

  public init(file: MappedFile, range: Range<Int>) {
    self.file = file
    self.range = range
  }

  var basePointer: UnsafeRawPointer {
    file.pointer + range.lowerBound
  }
}
