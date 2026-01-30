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

extension TimeUnit {
  func toFlatBufferUnit() -> FTimeUnit {
    switch self {
    case .second: return .second
    case .millisecond: return .millisecond
    case .microsecond: return .microsecond
    case .nanosecond: return .nanosecond
    }
  }
}

extension ArrowField {

  /// Parses an `ArrowField` from the FlatBuffers representation.
  /// - Parameter field:
  /// - Returns: The `ArrowField`.
  static func parse(from field: FField) throws(ArrowError) -> Self {
    let fieldType: ArrowType = try .parse(from: field)
    guard let fieldName = field.name else {
      throw .init(.invalid("Field name not found"))
    }
    let fieldMetadata = field.customMetadata
      .reduce(into: [String: String]()) { dict, kv in
        let key = kv.key
        guard let key else { return }
        dict[key] = kv.value
      }
    return .init(
      name: fieldName,
      dataType: fieldType,
      isNullable: field.nullable,
      metadata: fieldMetadata
    )
  }
}

extension ArrowType {

  /// Parses the `ArrowType` from a FlatBuffers `Field`.
  /// - Parameter field: The FlatBuffers `Field`.
  /// - Returns: The `ArrowType`, including all nested fields which are parsed recursively.
  /// - Throws: An `ArrowError` if parsing fails.
  static func parse(from field: FField) throws(ArrowError) -> Self {
    let type = field.typeType
    switch type {
    case .int:
      guard let intType = field.type(type: FInt.self) else {
        throw .init(.invalid("Could not get integer type from \(field)"))
      }
      let bitWidth = intType.bitWidth
      if bitWidth == 8 {
        if intType.isSigned {
          return .int8
        } else {
          return .uint8
        }
      }
      if bitWidth == 16 {
        return intType.isSigned ? .int16 : .uint16
      }
      if bitWidth == 32 {
        return intType.isSigned ? .int32 : .uint32
      }
      if bitWidth == 64 {
        return intType.isSigned ? .int64 : .uint64
      }
      throw .init(.invalid("Unhandled integer bit width: \(bitWidth)"))
    case .bool:
      return .boolean
    case .floatingpoint:
      guard let floatType = field.type(type: FFloatingPoint.self) else {
        throw .init(.invalid("Could not get floating point type from field"))
      }
      switch floatType.precision {
      case .half:
        return .float16
      case .single:
        return .float32
      case .double:
        return .float64
      }
    case .utf8:
      return .utf8
    case .largeutf8:
      return .largeUtf8
    case .utf8view:
      return .utf8View
    case .binary:
      return .binary
    case .largebinary:
      return .largeBinary
    case .binaryview:
      return .binaryView
    case .fixedsizebinary:
      guard let fType = field.type(type: FFixedSizeBinary.self) else {
        throw .init(
          .invalid("Could not get byteWidth from fixed binary field."))
      }
      return .fixedSizeBinary(fType.byteWidth)
    case .date:
      guard let dateType = field.type(type: FDate.self) else {
        throw .init(.invalid("Could not get date type from field"))
      }
      if dateType.unit == .day {
        return .date32
      }
      return .date64
    case .time:
      guard let timeType = field.type(type: FTime.self) else {
        throw .init(.invalid("Could not get time type from field"))
      }
      if timeType.unit == .second || timeType.unit == .millisecond {
        return .time32(
          timeType.unit == .second ? .second : .millisecond
        )
      }
      return .time64(
        timeType.unit == .microsecond ? .microsecond : .nanosecond
      )
    case .duration:
      guard let durationType = field.type(type: FDuration.self) else {
        throw .init(.invalid("Could not get duration type from field"))
      }
      switch durationType.unit {
      case .second:
        return .duration(.second)
      case .millisecond:
        return .duration(.millisecond)
      case .microsecond:
        return .duration(.microsecond)
      case .nanosecond:
        return .duration(.nanosecond)
      }
    case .timestamp:
      guard let timestampType = field.type(type: FTimestamp.self) else {
        throw .init(.invalid("Could not get timestamp type from field"))
      }
      let arrowUnit: TimeUnit
      switch timestampType.unit {
      case .second:
        arrowUnit = .second
      case .millisecond:
        arrowUnit = .millisecond
      case .microsecond:
        arrowUnit = .microsecond
      case .nanosecond:
        arrowUnit = .nanosecond
      }
      let timezone = timestampType.timezone
      return .timestamp(arrowUnit, timezone)
    case .struct_:
      guard field.type(type: FStruct.self) != nil else {
        throw .init(.invalid("Could not get struct type from field"))
      }
      var fields: [ArrowField] = []
      for child in field.children {
        let arrowField = try ArrowField.parse(from: child)
        fields.append(arrowField)
      }
      return .strct(fields)
    case .list:
      guard field.children.count == 1 else {
        throw .init(.invalid("Expected list field to have exactly one child"))
      }
      let childField = field.children[0]
      let arrowField = try ArrowField.parse(from: childField)
      return .list(arrowField)
    case .largelist:
      guard field.children.count == 1 else {
        throw .init(.invalid("Expected list field to have exactly one child"))
      }
      let childField = field.children[0]
      let arrowField = try ArrowField.parse(from: childField)
      return .largeList(arrowField)
    case .fixedsizelist:
      guard field.children.count == 1 else {
        throw .init(
          .invalid(
            "Expected list field to have exactly one child. Found: \(field.children.count) children"
          ))
      }
      guard let fType = field.type(type: FFixedSizeList.self) else {
        throw .init(.invalid("Could not get type from fixed size list field."))
      }
      let listSize = fType.listSize
      let child = field.children[0]
      let arrowField = try ArrowField.parse(from: child)
      return .fixedSizeList(arrowField, listSize)
    case .map:
      guard let fType = field.type(type: FMap.self) else {
        throw .init(.invalid("Could not get type from map field."))
      }
      let keysSorted = fType.keysSorted
      guard field.children.count == 1 else {
        throw .init(.invalid("Expected map field to have exactly one child."))
      }
      let child = field.children[0]
      let arrowField = try ArrowField.parse(from: child)
      guard case .strct(let fields) = arrowField.type, fields.count == 2 else {
        throw .init(
          .invalid("Map child must be a struct with key and value fields."))
      }
      return .map(arrowField, keysSorted)
    default:
      throw .init(.invalid("Unhandled field type: \(field.typeType)"))
    }
  }

  /// Maps from `ArrowType` to FlatBuffers type.
  /// - Returns: The FlatBuffers type.
  func fType() throws(ArrowError) -> FType {
    switch self {
    case .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64:
      return .int
    case .float16, .float32, .float64:
      return .floatingpoint
    case .binary:
      return .binary
    case .largeBinary:
      return .largebinary
    case .binaryView:
      return .binaryview
    case .utf8:
      return .utf8
    case .largeUtf8:
      return .largeutf8
    case .utf8View:
      return .utf8view
    case .boolean:
      return .bool
    case .date32, .date64:
      return .date
    case .time32, .time64:
      return .time
    case .timestamp:
      return .timestamp
    case .duration:
      return .duration
    case .strct:
      return .struct_
    case .list:
      return .list
    case .largeList:
      return .largelist
    case .map:
      return .map
    case .fixedSizeBinary:
      return .fixedsizebinary
    case .fixedSizeList:
      return .fixedsizelist
    default:
      throw .init(.invalid("Unhandled field type: \(self)"))
    }
  }
}
