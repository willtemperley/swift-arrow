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

import Arrow
import BinaryParsing
import FlatBuffers
import Foundation

let fileMarker: [UInt8] = .init(Data("ARROW1".utf8))
let continuationMarker = UInt32(0xFFFF_FFFF)

/// A view over `Data` which backs an Arrow buffer.
struct FileDataBuffer {
  let data: Data
  let range: Range<Int>

  init(data: Data, range: Range<Int>) {
    self.data = data
    self.range = range
    precondition(range.lowerBound <= range.upperBound)
  }
}

/// A reader for the Arrow file format.
///
/// The Arrow file format supports  random access. The Arrow file format contains a header and footer
/// around the Arrow streaming format.
public struct ArrowReader {

  let data: Data

  /// Create an `ArrowReader` from a URL.
  ///
  /// - Parameter url: the file to read from.
  /// - Throws: a ParsingError if the file could not be read.
  public init(url: URL) throws {
    self.data = try Data(contentsOf: url, options: .mappedIfSafe)
    try validateFileMarker()
  }

  /// Create an `ArrowReader` from Arrow IPC data.
  ///
  /// - Parameter data: Arrow IPC format data (file or stream format).
  /// - Throws: a ParsingError if the data is not valid Arrow IPC format.
  public init(data: Data) throws {
    self.data = data
    try validateFileMarker()
  }

  private func validateFileMarker() throws {
    try data.withParserSpan { input in
      let marker = try [UInt8](parsing: &input, byteCount: 6)
      guard marker == fileMarker else {
        throw ArrowError(.invalid("Invalid Arrow file"))
      }
    }
  }

  public func read() throws -> (ArrowSchema, [RecordBatch]) {

    let footerData = try data.withParserSpan { input in
      let count = input.count
      try input.seek(toAbsoluteOffset: count - 10)
      let footerLength = try Int(parsingLittleEndian: &input, byteCount: 4)
      try input.seek(toAbsoluteOffset: count - 10 - footerLength)
      return try [UInt8](parsing: &input, byteCount: footerLength)
    }

    var footerBuffer = ByteBuffer(
      data: Data(footerData)
    )

    let footer: FFooter = getRoot(byteBuffer: &footerBuffer)

    guard let schema = footer.schema else {
      throw ArrowError(.invalid("Missing schema in footer"))
    }
    let arrowSchema = try Self.loadSchema(schema: schema)
    var recordBatches: [RecordBatch] = []

    // MARK: Record batch parsing
    for block in footer.recordBatches {

      let (message, offset) = try data.withParserSpan { input in
        // TODO: This could be factored into a random access API
        try input.seek(toAbsoluteOffset: block.offset)
        let marker = try UInt32(parsingLittleEndian: &input)
        if marker != continuationMarker {
          throw ArrowError(.invalid("Missing continuation marker."))
        }
        let messageLength = try UInt32(parsingLittleEndian: &input)
        let data = try [UInt8](parsing: &input, byteCount: Int(messageLength))
        // TODO: Not zero-copy. Maybe new API fixes this.
        var mbb = ByteBuffer(data: Data(data))
        let message: FMessage = getRoot(byteBuffer: &mbb)
        let offset = Int64(input.startPosition)
        return (message, offset)
      }

      guard message.headerType == .recordbatch else {
        throw ArrowError(
          .invalid(
            "Expected RecordBatch message, got: \(message.headerType)."))
      }

      guard let rbMessage = message.header(type: FRecordBatch.self) else {
        throw ArrowError(.invalid("Expected RecordBatch as message header"))
      }
      guard footer.schema != nil else {
        throw ArrowError(.invalid("Expected schema in footer"))
      }

      // MARK: Load batch.
      let recordBatch = try Self.loadRecordBatch(
        data: self.data,
        arrowSchema: arrowSchema,
        rbMessage: rbMessage,
        offset: offset,
      )
      recordBatches.append(recordBatch)
    }

    return (arrowSchema, recordBatches)
  }

  static func loadRecordBatch(
    data: Data,
    arrowSchema: ArrowSchema,
    rbMessage: FRecordBatch,
    offset: Int64
  ) throws -> RecordBatch {

    // MARK: Load arrays
    var arrays: [AnyArrowArrayProtocol] = .init()
    var nodeIndex: Int32 = 0
    var bufferIndex: Int32 = 0
    var variadicBufferIndex: Int32 = 0

    for field in arrowSchema.fields {

      let array = try Self.loadField(
        data: data,
        rbMessage: rbMessage,
        field: field,
        offset: offset,
        nodeIndex: &nodeIndex,
        bufferIndex: &bufferIndex,
        variadicBufferIndex: &variadicBufferIndex
      )
      arrays.append(array)
    }

    return .init(schema: arrowSchema, columns: arrays)
  }

  static func loadField(
    data: Data,
    rbMessage: FRecordBatch,
    field: ArrowField,
    offset: Int64,
    nodeIndex: inout Int32,
    bufferIndex: inout Int32,
    variadicBufferIndex: inout Int32,
  ) throws(ArrowError) -> AnyArrowArrayProtocol {
    guard nodeIndex < rbMessage.nodes.count,
      let node = rbMessage.nodes[ifInBounds: nodeIndex]
    else {
      throw ArrowError(.invalid("Missing node at index \(nodeIndex)"))
    }
    nodeIndex += 1
    let buffer0 = try nextBuffer(
      message: rbMessage,
      index: &bufferIndex,
      offset: offset,
      data: data
    )

    // MARK: Load arrays
    let nullCount = Int(node.nullCount)
    if nullCount > 0 && !field.isNullable {
      print("Nullability violated for field \(field.name)")
    }
    let length = Int(node.length)
    let nullBuffer: NullBuffer
    if nullCount == 0 {
      nullBuffer = AllValidNullBuffer(valueCount: length)
    } else if nullCount == length {
      nullBuffer = AllNullBuffer(valueCount: length)
    } else {
      nullBuffer = NullBufferIPC(
        buffer: buffer0, valueCount: length, nullCount: nullCount)
    }

    let arrowType = field.type
    if arrowType == .boolean {
      let buffer1 = try nextBuffer(
        message: rbMessage, index: &bufferIndex, offset: offset, data: data)
      let valueBuffer = NullBufferIPC(
        buffer: buffer1, valueCount: length, nullCount: nullCount)
      return ArrowArrayBoolean(
        offset: 0, length: length, nullBuffer: nullBuffer,
        valueBuffer: valueBuffer)
    } else if arrowType.isNumeric {
      let buffer1 = try nextBuffer(
        message: rbMessage, index: &bufferIndex, offset: offset, data: data)
      switch arrowType {
      case .float32:
        return makeFixedArray(
          length: length, elementType: Float.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .float64:
        return makeFixedArray(
          length: length, elementType: Double.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .int8:
        return makeFixedArray(
          length: length, elementType: Int8.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .uint8:
        return makeFixedArray(
          length: length, elementType: UInt8.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .int16:
        return makeFixedArray(
          length: length, elementType: Int16.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .uint16:
        return makeFixedArray(
          length: length, elementType: UInt16.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .int32:
        return makeFixedArray(
          length: length, elementType: Int32.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .uint32:
        return makeFixedArray(
          length: length, elementType: UInt32.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .int64:
        return makeFixedArray(
          length: length, elementType: Int64.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .uint64:
        return makeFixedArray(
          length: length, elementType: UInt64.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      default:
        throw .init(.notImplemented("\(arrowType)"))
      }
    } else if arrowType.isTemporal {
      let buffer1 = try nextBuffer(
        message: rbMessage, index: &bufferIndex, offset: offset, data: data)
      switch arrowType {
      case .date32:
        return makeFixedArray(
          length: length, elementType: Int32.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .date64:
        return makeFixedArray(
          length: length, elementType: Int64.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .time32(_):
        return makeFixedArray(
          length: length, elementType: UInt32.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .time64(_):
        return makeFixedArray(
          length: length, elementType: UInt64.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .timestamp(_, _):
        return makeFixedArray(
          length: length, elementType: Int64.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .duration(_):
        return makeFixedArray(
          length: length, elementType: Int64.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .interval(_):
        return makeFixedArray(
          length: length, elementType: Int32.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      default:
        throw .init(.notImplemented("\(arrowType)"))
      }
    } else if arrowType.isVariable {
      let buffer1 = try nextBuffer(
        message: rbMessage, index: &bufferIndex, offset: offset, data: data)
      let buffer2 = try nextBuffer(
        message: rbMessage, index: &bufferIndex, offset: offset, data: data)
      if arrowType == .utf8 {
        let offsetsBufferTyped = FixedWidthBufferIPC<Int32>(
          buffer: buffer1
        )
        let valueBufferTyped = VariableLengthBufferIPC<String, Int32>(
          buffer: buffer2
        )
        return ArrowArrayVariable<String, Int32>(
          length: length,
          nullBuffer: nullBuffer,
          offsetsBuffer: offsetsBufferTyped,
          valueBuffer: valueBufferTyped
        )
      } else if arrowType == .binary {
        let offsetsBufferTyped = FixedWidthBufferIPC<Int32>(
          buffer: buffer1
        )
        let valueBufferTyped = VariableLengthBufferIPC<Data, Int32>(
          buffer: buffer2)
        return ArrowArrayVariable<Data, Int32>(
          length: length,
          nullBuffer: nullBuffer,
          offsetsBuffer: offsetsBufferTyped,
          valueBuffer: valueBufferTyped
        )
      } else if arrowType == .largeBinary {
        let offsetsBufferTyped = FixedWidthBufferIPC<Int64>(
          buffer: buffer1
        )
        let valueBufferTyped = VariableLengthBufferIPC<Data, Int64>(
          buffer: buffer2)
        return ArrowArrayVariable<Data, Int64>(
          length: length,
          nullBuffer: nullBuffer,
          offsetsBuffer: offsetsBufferTyped,
          valueBuffer: valueBufferTyped
        )
      } else if arrowType == .largeUtf8 {
        let offsetsBufferTyped = FixedWidthBufferIPC<Int64>(
          buffer: buffer1
        )
        let valueBufferTyped = VariableLengthBufferIPC<String, Int64>(
          buffer: buffer2)
        return ArrowArrayVariable<String, Int64>(
          length: length,
          nullBuffer: nullBuffer,
          offsetsBuffer: offsetsBufferTyped,
          valueBuffer: valueBufferTyped
        )
      } else {
        throw .init(.notImplemented("\(arrowType)"))
      }
    } else if arrowType.isBinaryView {
      let viewsBuffer = try nextBuffer(
        message: rbMessage,
        index: &bufferIndex,
        offset: offset,
        data: data
      )
      let viewsBufferTyped = FixedWidthBufferIPC<BinaryView>(
        buffer: viewsBuffer)

      guard
        let variadicCount = rbMessage.variadicBufferCounts[
          ifInBounds: variadicBufferIndex]
      else {
        throw .init(.invalid("Unable to get variadic buffer count."))
      }
      variadicBufferIndex += 1

      switch arrowType {
      case .binaryView:
        var dataBuffers: [VariableLengthBufferIPC<Data, Int32>] = []
        for _ in 0..<variadicCount {
          let dataBuffer = try nextBuffer(
            message: rbMessage,
            index: &bufferIndex,
            offset: offset,
            data: data
          )
          let dataBufferTyped = VariableLengthBufferIPC<Data, Int32>(
            buffer: dataBuffer)
          dataBuffers.append(dataBufferTyped)
        }
        return ArrowArrayBinaryView<Data>(
          offset: 0,
          length: length,
          nullBuffer: nullBuffer,
          viewsBuffer: viewsBufferTyped,
          dataBuffers: dataBuffers
        )
      case .utf8View:
        var dataBuffers: [VariableLengthBufferIPC<String, Int32>] = []
        for _ in 0..<variadicCount {
          let dataBuffer = try nextBuffer(
            message: rbMessage,
            index: &bufferIndex,
            offset: offset,
            data: data
          )
          let dataBufferTyped = VariableLengthBufferIPC<String, Int32>(
            buffer: dataBuffer
          )
          dataBuffers.append(dataBufferTyped)
        }
        return ArrowArrayBinaryView<String>(
          offset: 0,
          length: length,
          nullBuffer: nullBuffer,
          viewsBuffer: viewsBufferTyped,
          dataBuffers: dataBuffers
        )
      default:
        throw .init(.notImplemented("\(arrowType)"))
      }
    } else if arrowType.isNested {
      switch arrowType {
      case .list(let childField), .map(let childField, _):
        // A map is simply a list of struct<k,v> items.
        return try loadListArray(
          offsetType: Int32.self,
          rbMessage: rbMessage,
          bufferIndex: &bufferIndex,
          offset: offset, data: data,
          childField: childField,
          nodeIndex: &nodeIndex,
          variadicBufferIndex: &variadicBufferIndex,
          length: length,
          nullBuffer: nullBuffer
        )
      case .largeList(let childField):
        return try loadListArray(
          offsetType: Int64.self,
          rbMessage: rbMessage,
          bufferIndex: &bufferIndex,
          offset: offset, data: data,
          childField: childField,
          nodeIndex: &nodeIndex,
          variadicBufferIndex: &variadicBufferIndex,
          length: length,
          nullBuffer: nullBuffer
        )
      case .fixedSizeList(let field, let listSize):
        let array: AnyArrowArrayProtocol = try loadField(
          data: data,
          rbMessage: rbMessage,
          field: field,
          offset: offset,
          nodeIndex: &nodeIndex,
          bufferIndex: &bufferIndex,
          variadicBufferIndex: &variadicBufferIndex
        )
        return ArrowFixedSizeListArray(
          length: length,
          listSize: Int(listSize),
          nullBuffer: nullBuffer,
          values: array
        )
      case .strct(let fields):
        var arrays: [(String, AnyArrowArrayProtocol)] = []
        for field in fields {
          let array = try loadField(
            data: data,
            rbMessage: rbMessage,
            field: field,
            offset: offset,
            nodeIndex: &nodeIndex,
            bufferIndex: &bufferIndex,
            variadicBufferIndex: &variadicBufferIndex
          )
          arrays.append((field.name, array))
        }
        return ArrowStructArray(
          length: length,
          nullBuffer: nullBuffer,
          fields: arrays
        )
      default:
        throw .init(.notImplemented("\(arrowType)"))
      }
    } else {
      // MARK: Unclassifiable types.
      if case .fixedSizeBinary(let byteWidth) = arrowType {
        let valueBuffer = try nextBuffer(
          message: rbMessage, index: &bufferIndex, offset: offset, data: data)
        let valueBufferTyped = VariableLengthBufferIPC<Data, Int32>(
          buffer: valueBuffer)
        return ArrowArrayFixedSizeBinary(
          length: length,
          byteWidth: Int(byteWidth),
          nullBuffer: nullBuffer,
          valueBuffer: valueBufferTyped
        )
      }
      throw .init(.notImplemented("\(arrowType)"))
    }
  }

  static func nextBuffer(
    message: FRecordBatch,
    index: inout Int32,
    offset: Int64,
    data: Data
  ) throws(ArrowError) -> FileDataBuffer {
    guard index < message.buffers.count,
      let buffer = message.buffers[ifInBounds: index]
    else {
      throw .init(
        .invalid(
          "Buffer index \(index) requested for message with \(message.buffers.count) buffers."
        ))
    }
    index += 1
    let startOffset = offset + buffer.offset
    let endOffset = startOffset + buffer.length
    let range = Int(startOffset)..<Int(endOffset)
    let fileDataBuffer = FileDataBuffer(data: data, range: range)
    return fileDataBuffer
  }

  static func makeFixedArray<T>(
    length: Int,
    elementType: T.Type,
    nullBuffer: NullBuffer,
    buffer: FileDataBuffer
  ) -> ArrowArrayNumeric<T> {
    let fixedBuffer = FixedWidthBufferIPC<T>(
      buffer: buffer
    )
    return ArrowArrayNumeric(
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: fixedBuffer
    )
  }

  private static func loadListArray<
    T: FixedWidthInteger & BitwiseCopyable & SignedInteger
  >(
    offsetType: T.Type,
    rbMessage: FRecordBatch,
    bufferIndex: inout Int32,
    offset: Int64,
    data: Data,
    childField: ArrowField,
    nodeIndex: inout Int32,
    variadicBufferIndex: inout Int32,
    length: Int,
    nullBuffer: any NullBuffer
  ) throws(ArrowError) -> any AnyArrowArrayProtocol {
    let buffer1 = try nextBuffer(
      message: rbMessage, index: &bufferIndex, offset: offset, data: data)
    let offsetsBuffer = FixedWidthBufferIPC<T>(buffer: buffer1)
    let array: AnyArrowArrayProtocol = try loadField(
      data: data,
      rbMessage: rbMessage,
      field: childField,
      offset: offset,
      nodeIndex: &nodeIndex,
      bufferIndex: &bufferIndex,
      variadicBufferIndex: &variadicBufferIndex
    )
    return try makeListArray(
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      values: array
    )
  }

  private static func makeListArray<OffsetType>(
    length: Int,
    nullBuffer: NullBuffer,
    offsetsBuffer: any FixedWidthBufferProtocol<OffsetType>,
    values: AnyArrowArrayProtocol
  ) throws(ArrowError) -> ArrowListArray<OffsetType> {
    let requiredBytes = (length + 1) * MemoryLayout<OffsetType>.stride
    guard offsetsBuffer.length >= requiredBytes else {
      throw ArrowError(
        .invalid(
          "Offsets buffer of length: \(offsetsBuffer.length) too small: need \(requiredBytes) bytes for \(length) lists"
        )
      )
    }
    // Verify last offset matches child array length
    let lastOffset = offsetsBuffer[length]
    guard lastOffset == values.length else {
      throw ArrowError(
        .invalid("Expected last offset to match child array length."))
    }
    return ArrowListArray(
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      values: values
    )
  }

  static func loadSchema(schema: FSchema) throws(ArrowError) -> ArrowSchema {
    let metadata = schema.customMetadata.reduce(into: [String: String]()) {
      dict, kv in
      guard let key = kv.key else { return }
      dict[key] = kv.value
    }
    var fields: [ArrowField] = []
    for field in schema.fields {
      let arrowField = try ArrowField.parse(from: field)
      fields.append(arrowField)
    }
    return ArrowSchema(fields, metadata: metadata)
  }

}
