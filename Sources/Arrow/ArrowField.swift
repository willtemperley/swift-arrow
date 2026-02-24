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

/// Describes a single column in an `ArrowSchema`.
///
/// An `ArrowSchema` is an ordered collection of
/// `ArrowField` objects. Fields contain:
/// * `name`: the name of the field
/// * `dataType`: the type of the field
/// * `isNullable`: if the field is nullable
/// * `metadata`: a map of key-value pairs containing additional custom metadata
///
/// Arrow Extension types, are encoded in `ArrowField`s metadata.
public struct ArrowField: Codable, Sendable {
  public var name: String
  public var type: ArrowType

  /// Indicates whether this `ArrowField` supports null values.
  ///
  /// If true, the field *may* contain null values.
  public var isNullable: Bool
  /// A map of key-value pairs containing additional custom meta data.
  public var metadata: [String: String]
}

// This avoids using dictionary ordering metadata when calculating equality.
extension ArrowField: Equatable {
  public static func == (lhs: ArrowField, rhs: ArrowField) -> Bool {
    lhs.name == rhs.name
      && lhs.type == rhs.type
      && lhs.isNullable == rhs.isNullable
      && lhs.metadata == rhs.metadata
  }
}

extension ArrowField {
  /// Default list member field name.
  public static let listFieldDefaultName = "item"

  /// Creates a new field with the given name, data type, nullability and metadata.
  public init(
    name: String,
    dataType: ArrowType,
    isNullable: Bool,
    metadata: [String: String] = [:]
  ) {
    self.name = name
    self.type = dataType
    self.isNullable = isNullable
    self.metadata = metadata
  }

  /// Creates a new `ArrowField` suitable for `ArrowType::List`.
  ///
  /// While not required, this method follows the convention of naming the
  /// `Field` `"item"`.
  public init(listFieldWith dataType: ArrowType, isNullable: Bool) {
    self.init(
      name: Self.listFieldDefaultName,
      dataType: dataType,
      isNullable: isNullable
    )
  }

//  /// Create a new `ArrowField` suitable for `ArrowType::Dictionary`.
//  public init(
//    dictWithName: String,
//    key: ArrowType,
//    value: ArrowType,
//    isNullable: Bool
//  ) {
//    precondition(
//      key.isDictionaryKeyType,
//      "\(key) is not a valid dictionary key"
//    )
//    let dataType: ArrowType = .dictionary(key, value)
//    self = Self(name: dictWithName, dataType: dataType, isNullable: isNullable)
//  }

  /// Create a new struct `ArrowField`.
  ///
  /// - `name`: the name of the field
  /// - `fields`: the description of each struct element
  /// - `isNullable`: if the [`DataType::Struct`] array is nullable
  public init(
    structWithName name: String, fields: ArrowFields, isNullable: Bool
  ) {
    self.init(name: name, dataType: .strct(fields), isNullable: isNullable)
  }

  /// Create a new `ArrowField` with `ArrowType::List`.
  ///
  /// - Parameters:
  ///   - name: The name of the field.
  ///   - value: the description of each list element.
  ///   - isNullable: true if the field is nullable.
  public init(list name: String, value: ArrowField, isNullable: Bool) {
    self.init(name: name, dataType: .list(value), isNullable: isNullable)
  }

  /// Create a new `ArrowField` with `ArrowType.LargeList`.
  ///
  /// - Parameters:
  ///   - name: The name of the field.
  ///   - value: the description of each list element.
  ///   - isNullable: true if the field is nullable.
  public init(
    largeListNamed name: String,
    value: ArrowField,
    isNullable: Bool,
  ) {
    self = Self(
      name: name, dataType: .largeList(value), isNullable: isNullable)
  }

  /// Create a new `ArrowField` with `ArrowType.FixedSizeList`.
  ///
  /// - Parameters:
  ///   - name: The name of the field.
  ///   - value: the description of each list element.
  ///   - size: the list size
  ///   - isNullable: true if the field is nullable.
  public init(
    fixedSizeListNamed name: String,
    value: ArrowField,
    size: Int32,
    isNullable: Bool,
  ) {
    self.init(
      name: name,
      dataType: .fixedSizeList(value, size),
      isNullable: isNullable
    )
  }

  /// Sets the optional custom metadata.
  @inlinable
  public mutating func setMetadata(metadata: [String: String]) {
    self.metadata = metadata
  }

  /// Sets the metadata of this `ArrowField` to be `metadata` and returns self.
  public mutating func withMetadata(metadata: [String: String]) -> Self {
    self.setMetadata(metadata: metadata)
    return self
  }

  /// Set the name of this `ArrowField`.
  @inlinable
  public mutating func setName(name: String) {
    self.name = name
  }

  /// Set the name of the `ArrowField` and returns self.
  public mutating func withName(name: String) -> Self {
    self.name = name
    return self
  }

  /// Set the data type of the `ArrowField`.
  @inlinable
  public mutating func setDataType(dataType: ArrowType) {
    self.type = dataType
  }

  /// Set the data type of the `ArrowField` and returns self.
  public mutating func withDataType(_ dataType: ArrowType) -> Self {
    self.type = dataType
    return self
  }

  /// Returns the extension type name of this `ArrowField`, if set.
  ///
  /// This returns the value of [`extensionTypeNameKey`], if set in
  /// [`Field::metadata`]. If the key is missing, there is no extension type
  /// name and this returns `None`.
  public var extensionTypeName: String? {
    self.metadata[extensionTypeNameKey]
  }

  /// Returns the extension type metadata of this `ArrowField`, if set.
  ///
  /// This returns the value of [`extensionTypeNameMetadataKey`], if set in
  /// [`Field::metadata`]. If the key is missing, there is no extension type
  /// metadata and this returns `None`.
  public var extensionTypeMetadata: String? {
    self.metadata[extensionTypeNameMetadataKey]
  }

  /// Set the nullability of this `ArrowField`.
  @inlinable
  public mutating func setNullable(_ isNullable: Bool) {
    self.isNullable = isNullable
  }

  /// Set the nulllability of the `ArrowField` and returns self.
  public mutating func withNullable(isNullable: Bool) -> Self {
    self.isNullable = isNullable
    return self
  }

  /// Returns whether this `ArrowField` has an ordered dictionary, if this is a dictionary type.
  @inlinable
  public var dictIsOrdered: Bool {
    switch self.type {
    case .dictionary(_, let isOrdered, _, _): return isOrdered
    default: return false
    }
  }

  /// Check to see if `self` is a superset of `other` field. Superset is defined as:
  ///
  /// * if nullability doesn't match, self needs to be nullable
  /// * self.metadata is a superset of other.metadata
  /// * all other fields are equal
  public func contains(other: ArrowField) -> Bool {
    self.name == other.name
      && self.type.contains(other.type)
      && self.dictIsOrdered == other.dictIsOrdered
      // self need to be nullable or both of them are not nullable
      && (self.isNullable || !other.isNullable)
      // make sure self.metadata is a superset of other.metadata
      && other.metadata.allSatisfy { (key, v1) in
        self.metadata[key].map { v2 in v1 == v2 } ?? false
      }
  }
}
