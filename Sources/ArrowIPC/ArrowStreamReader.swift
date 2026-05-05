// Copyright 2026 The Apache Software Foundation
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

class ArrowStreamReader {
  var arrowSchema: ArrowSchema?

  func readMessage(data: MappedFile, offset: inout Int64) throws -> FMessage? {

    let message: FMessage? = try data.withParserSpan { input in
      try input.seek(toAbsoluteOffset: offset)

      let header = try MessageHeader(parsing: &input)
      let messageLength = header.metadataLength

      if messageLength == 0 {
        return nil
      }
      let data = try [UInt8](parsing: &input, byteCount: Int(messageLength))

      // TODO: Not zero-copy. Maybe new API fixes this.
      var mbb = ByteBuffer(data: Data(data))
      let message: FMessage = getRoot(byteBuffer: &mbb)
      offset = Int64(input.startPosition)
      return message
    }
    return message
  }

  func read(data: MappedFile) throws -> [RecordBatch] {
    var offset: Int64 = 0
    var recordBatches: [RecordBatch] = []
    while true {
      let message = try readMessage(data: data, offset: &offset)
      guard let message else {
        return recordBatches
      }
      switch message.headerType {
      case .schema:
        guard let sMessage = message.header(type: FSchema.self) else {
          throw ArrowError(.invalid("Expected RecordBatch as message header"))
        }
        self.arrowSchema = try ArrowReader.loadSchema(schema: sMessage)
      case .recordbatch:
        guard let rbMessage = message.header(type: FRecordBatch.self) else {
          throw ArrowError(.invalid("Expected RecordBatch as message header"))
        }
        guard let arrowSchema else {
          throw ArrowError(.invalid("ArrowSchema not available."))
        }
        let recordBatch = try ArrowReader.loadRecordBatch(
          file: data,
          arrowSchema: arrowSchema,
          rbMessage: rbMessage,
          offset: offset,
        )
        offset += message.bodyLength
        recordBatches.append(recordBatch)
      default:
        throw ArrowError(.notImplemented("Unexpected message header type."))
      }
    }
    return recordBatches
  }
}
