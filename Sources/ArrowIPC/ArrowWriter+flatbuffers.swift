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
import FlatBuffers

extension ArrowWriter {

  /// Creeate an offsets array for key-value metadata entries.
  /// - Parameters:
  ///   - metadata: The String;String key-value metadata.
  ///   - fbb: The FlatBuffers builder.
  /// - Returns: The offsets to the key-value entries.
  func metadataOffsets(
    metadata: [String: String],
    fbb: inout FlatBufferBuilder
  ) -> [Offset] {
    var keyValueOffsets: [Offset] = []
    for (key, value) in metadata {
      let keyOffset = fbb.create(string: key)
      let valueOffset = fbb.create(string: value)
      let kvOffset = FKeyValue.createKeyValue(
        &fbb,
        keyOffset: keyOffset,
        valueOffset: valueOffset
      )
      keyValueOffsets.append(kvOffset)
    }
    return keyValueOffsets
  }

  func write(
    field: ArrowField,
    to fbb: inout FlatBufferBuilder,
  ) throws(ArrowError) -> Offset {
    // Create child fields first.
    var fieldsOffset: Offset?
    if case .strct(let fields) = field.type {
      var offsets: [Offset] = []
      for field in fields {
        let offset = try write(field: field, to: &fbb)
        offsets.append(offset)
      }
      fieldsOffset = fbb.createVector(ofOffsets: offsets)
    } else if case .list(let childField) = field.type {
      let offset = try write(field: childField, to: &fbb)
      fieldsOffset = fbb.createVector(ofOffsets: [offset])
    } else if case .largeList(let childField) = field.type {
      let offset = try write(field: childField, to: &fbb)
      fieldsOffset = fbb.createVector(ofOffsets: [offset])
    } else if case .fixedSizeList(let childField, _) = field.type {
      let offset = try write(field: childField, to: &fbb)
      fieldsOffset = fbb.createVector(ofOffsets: [offset])
    } else if case .map(let childField, _) = field.type {
      let offset = try write(field: childField, to: &fbb)
      fieldsOffset = fbb.createVector(ofOffsets: [offset])
    }
    // Create all strings and nested objects before startField.
    let nameOffset = fbb.create(string: field.name)
    let fieldTypeOffset = try append(arrowType: field.type, to: &fbb)
    // Create metadata vector before startField.
    let metadata = field.metadata
    let keyValueOffsets = metadataOffsets(metadata: metadata, fbb: &fbb)
    let customMetadataOffset = fbb.createVector(ofOffsets: keyValueOffsets)
    // Start the Field table.
    let startOffset = FField.startField(&fbb)
    FField.add(name: nameOffset, &fbb)
    FField.add(nullable: field.isNullable, &fbb)
    if let childrenOffset = fieldsOffset {
      FField.addVectorOf(children: childrenOffset, &fbb)
    }
    let typeType = try field.type.fType()
    FField.add(typeType: typeType, &fbb)
    FField.add(type: fieldTypeOffset, &fbb)
    FField.addVectorOf(customMetadata: customMetadataOffset, &fbb)
    return FField.endField(&fbb, start: startOffset)
  }

  /// Append the arrow type to the FlatBuffers builder.
  /// - Parameters:
  ///   - arrowType: The `ArrowType`.
  ///   - fbb: The FlatBuffers builder.
  /// - Returns: The offset to the newly appended arrow type.
  /// - Throws: An `ArrowError` if the type is not serializable.
  func append(
    arrowType: ArrowType,
    to fbb: inout FlatBufferBuilder,
  ) throws(ArrowError) -> Offset {
    switch arrowType {
    case .int8, .uint8:
      return FInt.createInt(&fbb, bitWidth: 8, isSigned: arrowType == .int8)
    case .int16, .uint16:
      return FInt.createInt(&fbb, bitWidth: 16, isSigned: arrowType == .int16)
    case .int32, .uint32:
      return FInt.createInt(&fbb, bitWidth: 32, isSigned: arrowType == .int32)
    case .int64, .uint64:
      return FInt.createInt(&fbb, bitWidth: 64, isSigned: arrowType == .int64)
    case .float16:
      return FFloatingPoint.createFloatingPoint(&fbb, precision: .half)
    case .float32:
      return FFloatingPoint.createFloatingPoint(&fbb, precision: .single)
    case .float64:
      return FFloatingPoint.createFloatingPoint(&fbb, precision: .double)
    case .utf8:
      return FUtf8.endUtf8(&fbb, start: FUtf8.startUtf8(&fbb))
    case .largeUtf8:
      return FLargeUtf8.endLargeUtf8(
        &fbb, start: FLargeUtf8.startLargeUtf8(&fbb))
    case .utf8View:
      return FUtf8View.endUtf8View(&fbb, start: FUtf8View.startUtf8View(&fbb))
    case .binary:
      return FBinary.endBinary(&fbb, start: FBinary.startBinary(&fbb))
    case .largeBinary:
      return FLargeBinary.endLargeBinary(
        &fbb, start: FLargeBinary.startLargeBinary(&fbb))
    case .binaryView:
      return FBinaryView.endBinaryView(
        &fbb, start: FBinaryView.startBinaryView(&fbb))
    case .fixedSizeBinary(let byteWidth):
      let startOffset = FFixedSizeBinary.startFixedSizeBinary(&fbb)
      FFixedSizeBinary.add(byteWidth: byteWidth, &fbb)
      return FFixedSizeBinary.endFixedSizeBinary(&fbb, start: startOffset)
    case .boolean:
      return FBool.endBool(&fbb, start: FBool.startBool(&fbb))
    case .date32:
      let startOffset = FDate.startDate(&fbb)
      FDate.add(unit: .day, &fbb)
      return FDate.endDate(&fbb, start: startOffset)
    case .date64:
      let startOffset = FDate.startDate(&fbb)
      FDate.add(unit: .millisecond, &fbb)
      return FDate.endDate(&fbb, start: startOffset)
    case .time32(let unit):
      let startOffset = FTime.startTime(&fbb)
      FTime.add(unit: unit == .second ? .second : .millisecond, &fbb)
      FTime.add(bitWidth: 32, &fbb)
      return FTime.endTime(&fbb, start: startOffset)
    case .time64(let unit):
      let startOffset = FTime.startTime(&fbb)
      FTime.add(unit: unit == .microsecond ? .microsecond : .nanosecond, &fbb)
      FTime.add(bitWidth: 64, &fbb)
      return FTime.endTime(&fbb, start: startOffset)
    case .timestamp(let unit, let timezone):
      // Timezone string must be created before starting the timestamp table.
      let timezoneOffset: Offset? = timezone.map { fbb.create(string: $0) }
      let startOffset = FTimestamp.startTimestamp(&fbb)
      let fbUnit: FTimeUnit
      switch unit {
      case .second:
        fbUnit = .second
      case .millisecond:
        fbUnit = .millisecond
      case .microsecond:
        fbUnit = .microsecond
      case .nanosecond:
        fbUnit = .nanosecond
      }
      FTimestamp.add(unit: fbUnit, &fbb)
      if let timezoneOffset {
        FTimestamp.add(timezone: timezoneOffset, &fbb)
      }
      return FTimestamp.endTimestamp(&fbb, start: startOffset)
    case .duration(let timeUnit):
      let startOffset = FDuration.startDuration(&fbb)
      FDuration.add(unit: timeUnit.toFlatBufferUnit(), &fbb)
      return FDuration.endDuration(&fbb, start: startOffset)
    case .strct:
      let startOffset = FStruct.startStruct_(&fbb)
      return FStruct.endStruct_(&fbb, start: startOffset)
    case .list:
      let startOffset = FList.startList(&fbb)
      return FList.endList(&fbb, start: startOffset)
    case .largeList:
      let startOffset = FLargeList.startLargeList(&fbb)
      return FLargeList.endLargeList(&fbb, start: startOffset)
    case .fixedSizeList(_, let listSize):
      let startOffset = FFixedSizeList.startFixedSizeList(&fbb)
      FFixedSizeList.add(listSize: listSize, &fbb)
      return FFixedSizeList.endFixedSizeList(&fbb, start: startOffset)
    case .map:
      let startOffset = FMap.startMap(&fbb)
      return FMap.endMap(&fbb, start: startOffset)
    default:
      throw .init(
        .unknownType(
          "Unable to add FlatBuffers type for Arrow type: \(arrowType)."))
    }
  }
}
