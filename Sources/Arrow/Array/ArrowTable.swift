// Copyright 2025 The Apache Software Foundation
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

public class ArrowTable {
  public let schema: ArrowSchema
  public let rowCount: Int
  public let columns: [ArrowColumn]
  public init(_ schema: ArrowSchema, columns: [ArrowColumn]) {
    self.schema = schema
    self.columns = columns
    self.rowCount = columns[0].length
  }

  /// Create an ArrowTable from a 'RecordBatch' list.
  /// - Parameter recordBatches: The record batches.
  /// - Parameter schema: An optional schema, defaulting to the record batch schema.
  /// - Returns: An `ArrowResult` holding an `ArrowTable` on success.
  /// - Throws: an `ArrowError` if arrays have no elements or if elements have mismatched types.
  public static func from(
    recordBatches: [RecordBatch],
    schema: ArrowSchema? = nil
  ) throws(ArrowError) -> ArrowTable {
    if recordBatches.isEmpty {
      throw .init(.arrayHasNoElements)
    }
    var holders: [[AnyArrowArrayProtocol]] = []
    let schema = schema ?? recordBatches[0].schema
    for recordBatch in recordBatches {
      for index in 0..<schema.fields.count {
        if holders.count <= index {
          holders.append([AnyArrowArrayProtocol]())
        }
        holders[index].append(recordBatch.arrays[index])
      }
    }
    let builder = ArrowTable.Builder()
    for index in 0..<schema.fields.count {
      let field = schema.fields[index]
      let column = try makeArrowColumn(
        for: field,
        arrays: holders[index]
      )
      builder.addColumn(column)
    }
    return builder.finish()
  }

  private static func makeArrowColumn(
    for field: ArrowField,
    arrays: [AnyArrowArrayProtocol]
  ) throws(ArrowError) -> ArrowColumn {
    // Dispatch based on the field's type, not the first holder
    switch field.type {
    case .int8:
      return try makeTypedColumn(field, arrays, type: Int8.self)
    case .int16:
      return try makeTypedColumn(field, arrays, type: Int16.self)
    case .int32:
      return try makeTypedColumn(field, arrays, type: Int32.self)
    case .int64:
      return try makeTypedColumn(field, arrays, type: Int64.self)
    case .uint8:
      return try makeTypedColumn(field, arrays, type: UInt8.self)
    case .uint16:
      return try makeTypedColumn(field, arrays, type: UInt16.self)
    case .uint32:
      return try makeTypedColumn(field, arrays, type: UInt32.self)
    case .uint64:
      return try makeTypedColumn(field, arrays, type: UInt64.self)
//    case .float16:
//      return try makeTypedColumn(field, arrays, type: Float16.self)
    case .float32:
      return try makeTypedColumn(field, arrays, type: Float.self)
    case .float64:
      return try makeTypedColumn(field, arrays, type: Double.self)
    case .binary:
      return try makeTypedColumn(field, arrays, type: Data.self)
    case .utf8:
      return try makeTypedColumn(field, arrays, type: String.self)
    case .boolean:
      return try makeTypedColumn(field, arrays, type: Bool.self)
    case .date32, .date64:
      return try makeTypedColumn(field, arrays, type: Date.self)
    // TODO: make a fuzzer to make sure all types are hit

    case .strct, .list:
      return ArrowColumn(
        field,
        chunked: try AnyChunkedArray(arrays)
      )
    default:
      throw .init(.unknownType("Unsupported type: \(field.type)"))
    }
  }

  private static func makeTypedColumn<T>(
    _ field: ArrowField,
    _ holders: [AnyArrowArrayProtocol],
    type: T.Type
  ) throws(ArrowError) -> ArrowColumn {
    var arrays: [any ArrowArrayProtocol<T>] = []
    for holder in holders {
      guard let array = holder as? (any ArrowArrayProtocol<T>) else {
        throw .init(
          .runtimeError(
            "Array type mismatch: expected \(T.self) for field \(field.name)"
          ))
      }
      arrays.append(array)
    }
    return ArrowColumn(
      field,
      chunked: try ChunkedArray<T>(arrays)
    )
  }

  public class Builder {
    let schemaBuilder = ArrowSchema.Builder()
    var columns: [ArrowColumn] = []

    public init() {}

    //    @discardableResult
    //    public func addColumn<T>(
    //      _ fieldName: String,
    //      arrowArray: any ArrowArray<T>
    //    ) throws -> Builder {
    //      self.addColumn(fieldName, chunked: try ChunkedArrayX([arrowArray]))
    //    }

    //    @discardableResult
    //    public func addColumn<T>(
    //      _ fieldName: String,
    //      chunked: ChunkedArray<T>
    //    ) -> Builder {
    //      let field = ArrowField(
    //        name: fieldName,
    //        dataType: chunked.type,
    //        isNullable: chunked.nullCount != 0
    //      )
    //      self.schemaBuilder.addField(field)
    //      let column = ArrowColumn(field, chunked: chunked)
    //      self.columns.append(column)
    //      return self
    //    }

    @discardableResult
    public func addColumn<T>(
      _ field: ArrowField,
      arrowArray: any ArrowArrayProtocol<T>
    ) throws -> Builder {
      self.schemaBuilder.addField(field)
      let holder = try ChunkedArray([arrowArray])
      self.columns.append(ArrowColumn(field, chunked: holder))
      return self
    }

    @discardableResult
    public func addColumn<T>(
      _ field: ArrowField,
      chunked: ChunkedArray<T>
    ) -> Builder {
      self.schemaBuilder.addField(field)
      let column = ArrowColumn(field, chunked: chunked)
      self.columns.append(column)
      return self
    }

    @discardableResult
    public func addColumn(_ column: ArrowColumn) -> Builder {
      self.schemaBuilder.addField(column.field)
      self.columns.append(column)
      return self
    }

    public func finish() -> ArrowTable {
      ArrowTable(self.schemaBuilder.finish(), columns: self.columns)
    }
  }
}
