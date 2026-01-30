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

import Arrow
import FlatBuffers
import Foundation

public struct ArrowWriter {

  let url: URL
  var data: Data = .init()

  public init(url: URL) {
    self.url = url
    write(bytes: fileMarker)
  }

  public mutating func finish() throws {
    data.append(contentsOf: fileMarker)
    try data.write(to: url)
  }

  func padded(byteCount: Int, alignment: Int = 8) -> Int {
    let padding = byteCount % alignment
    if padding > 0 {
      return byteCount + alignment - padding
    }
    return byteCount
  }

  mutating func pad(alignment: Int = 8) {
    let remainder = data.count % alignment
    if remainder > 0 {
      let padding = alignment - remainder
      data.append(contentsOf: [UInt8](repeating: 0, count: padding))
    }
  }

  mutating func write(bytes: [UInt8], alignment: Int = 8) {
    data.append(contentsOf: fileMarker)
    let remainder = bytes.count % alignment
    if remainder > 0 {
      let padding = alignment - remainder
      data.append(contentsOf: [UInt8](repeating: 0, count: padding))
    }
    precondition(data.count % 8 == 0, "File must be aligned to 8 bytes.")
  }

  mutating func write(data other: Data, alignment: Int = 8) {
    self.data.append(other)
    let remainder = data.count % alignment
    if remainder > 0 {
      let padding = alignment - remainder
      data.append(contentsOf: [UInt8](repeating: 0, count: padding))
    }
    precondition(data.count % 8 == 0, "File must be aligned to 8 bytes.")
  }

  public mutating func write(
    schema: ArrowSchema,
    recordBatches: [RecordBatch]
  ) throws {

    try write(schema: schema)
    precondition(data.count % 8 == 0)
    let blocks = try write(recordBatches: recordBatches)

    precondition(data.count % 8 == 0)
    let footerOffset = data.count
    let footerData = try writeFooter(schema: schema, blocks: blocks)
    write(data: footerData)
    precondition(data.count % 8 == 0)
    withUnsafeBytes(of: Int32.zero.littleEndian) { val in
      data.append(contentsOf: val)
    }
    let footerLength = data.count - footerOffset
    withUnsafeBytes(of: Int32(footerLength).littleEndian) { val in
      data.append(contentsOf: val)
    }
  }

  mutating func write(schema: ArrowSchema) throws(ArrowError) {
    var fbb: FlatBufferBuilder = .init()
    let schemaOffset = try write(schema: schema, to: &fbb)
    fbb.finish(offset: schemaOffset)
    self.write(data: fbb.data)
  }

  /// Write the schema to file.
  /// - Parameters:
  ///   - schema:The Arrow schema
  ///   - fbb: the FlatBuffers builder to append the schema this to.
  /// - Returns: The FlatBuffers offset.
  /// - Throws: An `ArrowError` if type conversion is unable to continue.
  private func write(
    schema: ArrowSchema,
    to fbb: inout FlatBufferBuilder
  ) throws(ArrowError) -> Offset {
    var fieldOffsets: [Offset] = []
    for field in schema.fields {
      let offset = try write(field: field, to: &fbb)
      fieldOffsets.append(offset)
    }
    let fieldsOffset: Offset = fbb.createVector(ofOffsets: fieldOffsets)
    let metadataOffset = {
      if let metadata = schema.metadata, !metadata.isEmpty {
        let metadataOffsets = metadataOffsets(metadata: metadata, fbb: &fbb)
        return fbb.createVector(ofOffsets: metadataOffsets)
      } else {
        return Offset()
      }
    }()
    let schemaOffset = FSchema.createSchema(
      &fbb,
      endianness: .little,
      fieldsVectorOffset: fieldsOffset,
      customMetadataVectorOffset: metadataOffset
    )
    return schemaOffset
  }

  private func writeFooter(
    schema: ArrowSchema,
    blocks: [FBlock]
  ) throws(ArrowError) -> Data {
    var fbb: FlatBufferBuilder = .init()
    let schemaOffset = try write(schema: schema, to: &fbb)
    let blocksOffset = fbb.createVector(ofStructs: blocks)
    let footerStartOffset = FFooter.startFooter(&fbb)
    FFooter.add(schema: schemaOffset, &fbb)
    FFooter.addVectorOf(recordBatches: blocksOffset, &fbb)
    let footerOffset = FFooter.endFooter(&fbb, start: footerStartOffset)
    fbb.finish(offset: footerOffset)
    return fbb.data
  }

  // MARK: Record batch methods.

  private mutating func write(
    recordBatches: [RecordBatch]
  ) throws -> [FBlock] {
    var blocks: [FBlock] = .init()
    for recordBatch in recordBatches {
      let startIndex = data.count
      let message = try write(batch: recordBatch)
      var buffer = Data()
      withUnsafeBytes(of: continuationMarker.littleEndian) { val in
        buffer.append(contentsOf: val)
      }
      withUnsafeBytes(of: UInt32(message.count).littleEndian) { val in
        buffer.append(contentsOf: val)
      }
      write(data: buffer)
      write(data: message)
      precondition(data.count % 8 == 0)
      let metadataLength = data.count - startIndex
      let bodyStart = data.count

      try writeRecordBatchData(
        fields: recordBatch.schema.fields,
        arrays: recordBatch.arrays
      )
      precondition(data.count % 8 == 0)

      let bodyLength = data.count - bodyStart
      let expectedSize = startIndex + metadataLength + bodyLength
      guard expectedSize == data.count else {
        throw ArrowError(
          .invalid(
            "Invalid Block. Expected \(expectedSize), got \(data.count)"
          ))
      }
      let block = FBlock(
        offset: Int64(startIndex),
        metaDataLength: Int32(metadataLength),
        bodyLength: Int64(bodyLength)
      )
      blocks.append(block)
    }
    return blocks
  }

  private mutating func writeRecordBatchData(
    fields: [ArrowField],
    arrays: [AnyArrowArrayProtocol]
  ) throws(ArrowError) {
    for index in 0..<fields.count {

      let array = arrays[index]
      let field = fields[index]
      let buffers = array.buffers

      for buffer in buffers {
        buffer.withUnsafeBytes { ptr in
          data.append(contentsOf: ptr)
        }
        pad()
        precondition(data.count % 8 == 0, "Data size must be multiple of 8")
      }
      if field.type.isNested {
        switch field.type {
        case .strct(let fields):
          guard let structArray = array as? ArrowStructArray else {
            throw .init(.invalid("Struct type array expected for nested type"))
          }
          try writeRecordBatchData(
            fields: fields,
            arrays: structArray.fields.map(\.array)
          )
        case .list(let childType), .largeList(let childType),
          .map(let childType, _):
          guard let listArray = array as? ListArrayProtocol else {
            throw .init(.invalid("List type array expected."))
          }
          try writeRecordBatchData(
            fields: [childType],
            arrays: [listArray.values]
          )
        case .fixedSizeList(let childType, _):
          guard let listArray = array as? ArrowFixedSizeListArray else {
            throw .init(.invalid("Fixed size list type array expected."))
          }
          try writeRecordBatchData(
            fields: [childType],
            arrays: [listArray.values]
          )
        default:
          throw .init(.notImplemented("\(field.type)"))
        }
      }
    }
  }

  /// Write the record batch message.
  /// - Parameter batch: The `RecordBatch` to write metadata for.
  /// - Returns: The FlatBuffers message serialized to `Data`.
  /// - Throws: An `ArrowError`if arrays are unreadable.
  private func write(
    batch: RecordBatch
  ) throws(ArrowError) -> Data {
    let schema = batch.schema
    var fbb = FlatBufferBuilder()

    // MARK: Field nodes.
    var fieldNodes: [FFieldNode] = []
    try writeFieldNodes(
      fields: schema.fields,
      arrays: batch.arrays,
      nodes: &fieldNodes,
    )
    let nodeOffset = fbb.createVector(ofStructs: fieldNodes)

    // MARK: Buffers.
    var buffers: [FBuffer] = []
    var variadicBufferCounts: [Int64] = []
    var bufferOffset: Int64 = 0
    try writeBufferInfo(
      schema.fields,
      arrays: batch.arrays,
      bufferOffset: &bufferOffset,
      buffers: &buffers,
      variadicBufferCounts: &variadicBufferCounts
    )
    let batchBuffersOffset = fbb.createVector(ofStructs: buffers)
    let variadicCountsOffset = fbb.createVector(variadicBufferCounts)

    // MARK: Start record batch.
    let startRb = FRecordBatch.startRecordBatch(&fbb)
    FRecordBatch.addVectorOf(nodes: nodeOffset, &fbb)
    FRecordBatch.addVectorOf(buffers: batchBuffersOffset, &fbb)
    FRecordBatch.add(length: Int64(batch.length), &fbb)
    FRecordBatch.addVectorOf(variadicBufferCounts: variadicCountsOffset, &fbb)

    let recordBatchOffset = FRecordBatch.endRecordBatch(
      &fbb,
      start: startRb
    )
    let bodySize = Int64(bufferOffset)
    let startMessage = FMessage.startMessage(&fbb)
    FMessage.add(version: .max, &fbb)
    FMessage.add(bodyLength: Int64(bodySize), &fbb)
    FMessage.add(headerType: .recordbatch, &fbb)
    FMessage.add(header: recordBatchOffset, &fbb)
    let messageOffset = FMessage.endMessage(&fbb, start: startMessage)
    fbb.finish(offset: messageOffset)
    return fbb.data
  }

  /// Write the field nodes.
  /// - Parameters:
  ///   - fields: The Arrow fields.
  ///   - arrays: The Arrow arrays.
  ///   - nodes: The field node array being written to.
  private func writeFieldNodes(
    fields: [ArrowField],
    arrays: [AnyArrowArrayProtocol],
    nodes: inout [FFieldNode]
  ) throws(ArrowError) {
    for index in 0..<fields.count {
      let column = arrays[index]
      let field = fields[index]
      let fieldNode = FFieldNode(
        length: Int64(column.length),
        nullCount: Int64(column.nullCount)
      )
      nodes.append(fieldNode)
      if field.type.isNested {
        switch field.type {
        case .strct(let fields):
          if let column = column as? ArrowStructArray {
            try writeFieldNodes(
              fields: fields,
              arrays: column.fields.map(\.array),
              nodes: &nodes,
            )
          }
        case .list(let childField), .largeList(let childField),
          .map(let childField, _):
          if let column = column as? ListArrayProtocol {
            try writeFieldNodes(
              fields: [childField],
              arrays: [column.values],
              nodes: &nodes,
            )
          }
        case .fixedSizeList(let childField, _):
          if let column = column as? ArrowFixedSizeListArray {
            try writeFieldNodes(
              fields: [childField],
              arrays: [column.values],
              nodes: &nodes,
            )
          }
        default:
          throw .init(.notImplemented("Unhandled field type: \(field.type)"))
        }
      }
    }
  }

  private func writeBufferInfo(
    _ fields: [ArrowField],
    arrays: [AnyArrowArrayProtocol],
    bufferOffset: inout Int64,
    buffers: inout [FBuffer],
    variadicBufferCounts: inout [Int64]
  ) throws(ArrowError) {
    for index in 0..<fields.count {
      let array = arrays[index]
      let field = fields[index]

      // Write all buffers for this field
      for bufferDataSize in array.bufferSizes {
        let buffer = FBuffer(
          offset: Int64(bufferOffset),
          length: Int64(bufferDataSize)
        )
        buffers.append(buffer)
        // Advance by padded amount
        bufferOffset += Int64(padded(byteCount: bufferDataSize))
      }
      if field.type.isBinaryView {
        variadicBufferCounts.append(Int64(array.buffers.count - 2))
      }
      // After writing this field's buffers, recurse into children
      if field.type.isNested {
        switch field.type {
        case .strct(let fields):
          guard let column = array as? ArrowStructArray else {
            throw .init(.invalid("Expected ArrowStructArray for nested struct"))
          }
          try writeBufferInfo(
            fields,
            arrays: column.fields.map(\.array),
            bufferOffset: &bufferOffset,
            buffers: &buffers,
            variadicBufferCounts: &variadicBufferCounts
          )
        case .list(let childField), .largeList(let childField),
          .map(let childField, _):
          guard let column = array as? ListArrayProtocol else {
            throw .init(.invalid("Expected list array."))
          }
          try writeBufferInfo(
            [childField],
            arrays: [column.values],
            bufferOffset: &bufferOffset,
            buffers: &buffers,
            variadicBufferCounts: &variadicBufferCounts
          )
        case .fixedSizeList(let childField, _):
          guard let column = array as? ArrowFixedSizeListArray else {
            throw .init(.invalid("Expected ArrowFixedSizeListArray"))
          }
          try writeBufferInfo(
            [childField],
            arrays: [column.values],
            bufferOffset: &bufferOffset,
            buffers: &buffers,
            variadicBufferCounts: &variadicBufferCounts
          )
        default:
          throw .init(.notImplemented("Unsupported type: \(field.type)"))
        }
      }
    }
  }
}
