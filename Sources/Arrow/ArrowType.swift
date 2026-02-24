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

public typealias Time32 = Int32
public typealias Time64 = Int64
public typealias Date32 = Int32
public typealias Date64 = Int64
public typealias Timestamp = Int64

public typealias ArrowFields = [ArrowField]

public struct UnionField: Codable, Sendable, Equatable {
  public let typeId: Int8
  public let field: ArrowField

  public init(typeId: Int8, field: ArrowField) {
    self.typeId = typeId
    self.field = field
  }
}

/// Datatypes _intended_ to be supported by this implementation of Apache Arrow.
///
/// This is *Work In Progress*. Many of these have not been implemented or tested yet.
///
/// The variants of this enum include primitive fixed size types as well as
/// parametric or nested types. See `Schema.fbs` for Arrow's specification.
///
/// # Nested Support
/// Currently, the Swift implementation supports the following nested types:
///  - `List<T>`
///  - `Struct<T, U, V, ...>`
///
/// The intention is to implement these:
///  - `FixedSizeList<T>`
///  - `Union<T, U, V, ...>`
///  - `LargeList<T>`
///  - `Map<K, V>`
///
/// Nested types can themselves be nested within other arrays.
/// For more information on these types please see
/// [the physical memory layout of Apache Arrow]
///
/// [`Schema.fbs`]: https://github.com/apache/arrow/blob/main/format/Schema.fbs
/// [the physical memory layout of Apache Arrow]: https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout
public indirect enum ArrowType: Codable, Sendable, Equatable {
  /// Null type
  case null  // TODO: Implement this
  /// A boolean datatype representing the values `true` and `false`.
  case boolean
  /// A signed 8-bit integer.
  case int8
  /// A signed 16-bit integer.
  case int16
  /// A signed 32-bit integer.
  case int32
  /// A signed 64-bit integer.
  case int64
  /// An unsigned 8-bit integer.
  case uint8
  /// An unsigned 16-bit integer.
  case uint16
  /// An unsigned 32-bit integer.
  case uint32
  /// An unsigned 64-bit integer.
  case uint64
  /// A 16-bit floating point number.
  case float16
  /// A 32-bit floating point number.
  case float32
  /// A 64-bit floating point number.
  case float64
  /// A timestamp with an optional timezone.
  ///
  /// Time is measured as a Unix epoch, counting the seconds from
  /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
  /// as a signed 64-bit integer.
  ///
  /// The time zone is a string indicating the name of a time zone, one of:
  ///
  /// * As used in the Olson time zone database (the "tz database" or
  ///   "tzdata"), such as "America/New_York"
  /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
  ///
  /// Timestamps with a non-empty timezone
  /// ------------------------------------
  ///
  /// If a Timestamp column has a non-empty timezone value, its epoch is
  /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in the *UTC* timezone
  /// (the Unix epoch), regardless of the Timestamp's own timezone.
  ///
  /// Therefore, timestamp values with a non-empty timezone correspond to
  /// physical points in time together with some additional information about
  /// how the data was obtained and/or how to display it (the timezone).
  ///
  ///   For example, the timestamp value 0 with the timezone string "Europe/Paris"
  ///   corresponds to "January 1st 1970, 00h00" in the UTC timezone, but the
  ///   application may prefer to display it as "January 1st 1970, 01h00" in
  ///   the Europe/Paris timezone (which is the same physical point in time).
  ///
  /// One consequence is that timestamp values with a non-empty timezone
  /// can be compared and ordered directly, since they all share the same
  /// well-known point of reference (the Unix epoch).
  ///
  /// # Timestamps with an unset / empty timezone
  ///
  /// If a Timestamp column has no timezone value, its epoch is
  /// 1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone.
  ///
  /// Therefore, timestamp values without a timezone cannot be meaningfully
  /// interpreted as physical points in time, but only as calendar / clock
  /// indications ("wall clock time") in an unspecified timezone.
  ///
  ///   For example, the timestamp value 0 with an empty timezone string
  ///   corresponds to "January 1st 1970, 00h00" in an unknown timezone: there
  ///   is not enough information to interpret it as a well-defined physical
  ///   point in time.
  ///
  /// One consequence is that timestamp values without a timezone cannot
  /// be reliably compared or ordered, since they may have different points of
  /// reference.  In particular, it is *not* possible to interpret an unset
  /// or empty timezone as the same as "UTC".
  ///
  /// Conversion between timezones
  ///
  /// If a Timestamp column has a non-empty timezone, changing the timezone
  /// to a different non-empty value is a metadata-only operation:
  /// the timestamp values need not change as their point of reference remains
  /// the same (the Unix epoch).
  ///
  /// However, if a Timestamp column has no timezone value, changing it to a
  /// non-empty value requires to think about the desired semantics.
  /// One possibility is to assume that the original timestamp values are
  /// relative to the epoch of the timezone being set; timestamp values should
  /// then adjusted to the Unix epoch (for example, changing the timezone from
  /// empty to "Europe/Paris" would require converting the timestamp values
  /// from "Europe/Paris" to "UTC", which seems counter-intuitive but is
  /// nevertheless correct).
  ///
  ///
  // TODO: decide on what kind of timezone support should be added.
  case timestamp(TimeUnit, String?)
  /// A signed 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
  /// in days.
  case date32
  /// A signed 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
  /// in milliseconds.
  ///
  /// # Valid Ranges
  ///
  /// According to the Arrow specification ([Schema.fbs]), values of Date64
  /// are treated as the number of *days*, in milliseconds, since the UNIX
  /// epoch. Therefore, values of this type  must be evenly divisible by
  /// `86_400_000`, the number of milliseconds in a standard day.
  ///
  /// It is not valid to store milliseconds that do not represent an exact
  /// day. The reason for this restriction is compatibility with other
  /// language's native libraries (specifically Java), which historically
  /// lacked a dedicated date type and only supported timestamps.
  ///
  /// # Validation
  ///
  /// This library does not validate or enforce that Date64 values are evenly
  /// divisible by `86_400_000`  for performance and usability reasons. Date64
  /// values are treated similarly to `Timestamp(TimeUnit::Millisecond,
  /// None)`: values will be displayed with a time of day if the value does
  /// not represent an exact day, and arithmetic will be done at the
  /// millisecond granularity.
  ///
  /// # Recommendation
  ///
  /// Users should prefer [`Date32`] to cleanly represent the number
  /// of days, or one of the Timestamp variants to include time as part of the
  /// representation, depending on their use case.
  ///
  /// # Further Reading
  ///
  /// For more details, see [#5288](https://github.com/apache/arrow-rs/issues/5288).
  ///
  /// [`Date32`]: Self::Date32
  /// [Schema.fbs]: https://github.com/apache/arrow/blob/main/format/Schema.fbs
  case date64
  /// A signed 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
  /// Must be either seconds or milliseconds.
  case time32(TimeUnit)
  /// A signed 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
  /// Must be either microseconds or nanoseconds.
  case time64(TimeUnit)
  /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
  case duration(TimeUnit)
  /// A "calendar" interval which models types that don't necessarily
  /// have a precise duration without the context of a base timestamp (e.g.
  /// days can differ in length during day light savings time transitions).
  case interval(IntervalUnit)
  /// Opaque binary data of variable length.
  ///
  /// A single Binary array can store up to [`i32::MAX`] bytes
  /// of binary data in total.
  case binary
  /// Opaque binary data of fixed size.
  ///
  /// Enum parameter specifies the number of bytes per value.
  case fixedSizeBinary(Int32)
  /// Opaque binary data of variable length and 64-bit offsets.
  ///
  /// A single LargeBinary array can store up to [`i64::MAX`] bytes
  /// of binary data in total.
  case largeBinary
  /// Opaque binary data of variable length.
  ///
  /// Logically the same as [`Binary`], but the internal representation uses a view
  /// struct that contains the string length and either the string's entire data
  /// inline (for small strings) or an inlined prefix, an index of another buffer,
  /// and an offset pointing to a slice in that buffer (for non-small strings).
  ///
  /// [`Binary`]: Self::Binary
  case binaryView
  /// A variable-length string in Unicode with UTF-8 encoding.
  ///
  /// A single Utf8 array can store up to [`i32::MAX`] bytes
  /// of string data in total.
  case utf8
  /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
  ///
  /// A single LargeUtf8 array can store up to [`i64::MAX`] bytes
  /// of string data in total.
  case largeUtf8
  /// A variable-length string in Unicode with UTF-8 encoding
  ///
  /// Logically the same as [`Utf8`], but the internal representation uses a view
  /// struct that contains the string length and either the string's entire data
  /// inline (for small strings) or an inlined prefix, an index of another buffer,
  /// and an offset pointing to a slice in that buffer (for non-small strings).
  ///
  /// [`Utf8`]: Self::Utf8
  case utf8View
  /// A list of some logical data type with variable length.
  ///
  /// A single List array can store up to [`i32::MAX`] elements in total.
  case list(ArrowField)

  /// (NOT YET FULLY SUPPORTED)  A list of some logical data type with variable length.
  ///
  /// Logically the same as [`List`], but the internal representation differs in how child
  /// data is referenced, allowing flexibility in how data is layed out.
  ///
  /// Note this data type is not yet fully supported. Using it with arrow APIs may result in `panic`s.
  ///
  /// [`List`]: Self::List
  case listView(ArrowField)
  /// A list of some logical data type with fixed length.
  case fixedSizeList(ArrowField, Int32)
  /// A list of some logical data type with variable length and 64-bit offsets.
  ///
  /// A single LargeList array can store up to [`i64::MAX`] elements in total.
  case largeList(ArrowField)

  /// (NOT YET FULLY SUPPORTED)  A list of some logical data type with variable length and 64-bit offsets.
  ///
  /// Logically the same as [`LargeList`], but the internal representation differs in how child
  /// data is referenced, allowing flexibility in how data is layed out.
  ///
  /// Note this data type is not yet fully supported. Using it with arrow APIs may result in `panic`s.
  ///
  /// [`LargeList`]: Self::LargeList
  case largeListView(ArrowField)
  /// A nested datatype that contains a number of sub-fields.
  case strct(ArrowFields)
  /// A nested datatype that can represent slots of differing types. Components:
  ///
  /// 1. [`UnionFields`]
  /// 2. The type of union (Sparse or Dense)
  case union([UnionField], UnionMode)
  /// A dictionary encoded array (`key_type`, `value_type`), where
  /// each array element is an index of `key_type` into an
  /// associated dictionary of `value_type`.
  ///
  /// Dictionary arrays are used to store columns of `value_type`
  /// that contain many repeated values using less memory, but with
  /// a higher CPU overhead for some operations.
  ///
  /// This type mostly used to represent low cardinality string
  /// arrays or a limited set of primitive types as integers.
  case dictionary(id: Int64, isOrdered: Bool, key: ArrowType, value: ArrowType)
  /// Exact 32-bit width decimal value with precision and scale
  ///
  /// * precision is the total number of digits
  /// * scale is the number of digits past the decimal
  ///
  /// For example the number 123.45 has precision 5 and scale 2.
  ///
  /// In certain situations, scale could be negative number. For
  /// negative scale, it is the number of padding 0 to the right
  /// of the digits.
  ///
  /// For example the number 12300 could be treated as a decimal
  /// has precision 3 and scale -2.
  case decimal32(UInt8, Int8)
  /// Exact 64-bit width decimal value with precision and scale
  ///
  /// * precision is the total number of digits
  /// * scale is the number of digits past the decimal
  ///
  /// For example the number 123.45 has precision 5 and scale 2.
  ///
  /// In certain situations, scale could be negative number. For
  /// negative scale, it is the number of padding 0 to the right
  /// of the digits.
  ///
  /// For example the number 12300 could be treated as a decimal
  /// has precision 3 and scale -2.
  case decimal64(UInt8, Int8)
  /// Exact 128-bit width decimal value with precision and scale
  ///
  /// * precision is the total number of digits
  /// * scale is the number of digits past the decimal
  ///
  /// For example the number 123.45 has precision 5 and scale 2.
  ///
  /// In certain situations, scale could be negative number. For
  /// negative scale, it is the number of padding 0 to the right
  /// of the digits.
  ///
  /// For example the number 12300 could be treated as a decimal
  /// has precision 3 and scale -2.
  case decimal128(UInt8, Int8)
  /// Exact 256-bit width decimal value with precision and scale
  ///
  /// * precision is the total number of digits
  /// * scale is the number of digits past the decimal
  ///
  /// For example the number 123.45 has precision 5 and scale 2.
  ///
  /// In certain situations, scale could be negative number. For
  /// negative scale, it is the number of padding 0 to the right
  /// of the digits.
  ///
  /// For example the number 12300 could be treated as a decimal
  /// has precision 3 and scale -2.
  case decimal256(UInt8, Int8)
  /// A Map is a logical nested type that is represented as
  ///
  /// `List<entries: Struct<key: K, value: V>>`
  ///
  /// The keys and values are each respectively contiguous.
  /// The key and value types are not constrained, but keys should be
  /// hashable and unique.
  /// Whether the keys are sorted can be set in the `bool` after the `Field`.
  ///
  /// In a field with Map type, the field has a child Struct field, which then
  /// has two children: key type and the second the value type. The names of the
  /// child fields may be respectively "entries", "key", and "value", but this is
  /// not enforced.
  case map(ArrowField, Bool)
  /// A run-end encoding (REE) is a variation of run-length encoding (RLE). These
  /// encodings are well-suited for representing data containing sequences of the
  /// same value, called runs. Each run is represented as a value and an integer giving
  /// the index in the array where the run ends.
  ///
  /// A run-end encoded array has no buffers by itself, but has two child arrays. The
  /// first child array, called the run ends array, holds either 16, 32, or 64-bit
  /// signed integers. The actual values of each run are held in the second child array.
  ///
  /// These child arrays are prescribed the standard names of "run_ends" and "values"
  /// respectively.
  case runEndEncoded(ArrowField, ArrowField)
}

/// An absolute length of time in seconds, milliseconds, microseconds or nanoseconds.
public enum TimeUnit: Codable, Sendable {
  /// Time in seconds.
  case second
  /// Time in milliseconds.
  case millisecond
  /// Time in microseconds.
  case microsecond
  /// Time in nanoseconds.
  case nanosecond
}

/// YEAR_MONTH, DAY_TIME, MONTH_DAY_NANO interval in SQL style.
public enum IntervalUnit: Codable, Sendable {
  /// Indicates the number of elapsed whole months, stored as 4-byte integers.
  case yearMonth
  /// Indicates the number of elapsed days and milliseconds,
  /// stored as 2 contiguous 32-bit integers (days, milliseconds) (8-bytes in total).
  case dayTime
  /// A triple of the number of elapsed months, days, and nanoseconds.
  /// The values are stored contiguously in 16 byte blocks. Months and
  /// days are encoded as 32 bit integers and nanoseconds is encoded as a
  /// 64 bit integer. All integers are signed. Each field is independent
  /// (e.g. there is no constraint that nanoseconds have the same sign
  /// as days or that the quantity of nanoseconds represents less
  /// than a day's worth of time).
  case monthDayNano
}

/// Sparse or Dense union layouts.
public enum UnionMode: Codable, Sendable {
  /// Sparse union layout
  case sparse
  /// Dense union layout
  case dense
}

extension ArrowType: CustomStringConvertible {
  public var description: String {
    switch self {
    case .strct(let fields):
      var result = "Struct("
      if !fields.isEmpty {
        let fieldDescriptions = fields.map { "\($0.name): \($0.type)" }
        result += fieldDescriptions.joined(separator: ", ")
      }
      result += ")"
      return result
    case .null:
      return "Null"
    case .boolean:
      return "Boolean"
    case .int8:
      return "Int8"
    case .int16:
      return "Int16"
    case .int32:
      return "Int32"
    case .int64:
      return "Int64"
    case .uint8:
      return "UInt8"
    case .uint16:
      return "UInt16"
    case .uint32:
      return "UInt32"
    case .uint64:
      return "UInt64"
    case .float16:
      return "Float16"
    case .float32:
      return "Float32"
    case .float64:
      return "Float64"
    case .timestamp(let unit, let timezone):
      if let tz = timezone {
        return "Timestamp(\(unit), \(tz))"
      } else {
        return "Timestamp(\(unit))"
      }
    case .date32:
      return "Date32"
    case .date64:
      return "Date64"
    case .time32(let unit):
      return "Time32(\(unit))"
    case .time64(let unit):
      return "Time64(\(unit))"
    case .duration(let unit):
      return "Duration(\(unit))"
    case .interval(let unit):
      return "Interval(\(unit))"
    case .binary:
      return "Binary"
    case .fixedSizeBinary(let size):
      return "FixedSizeBinary(\(size))"
    case .largeBinary:
      return "LargeBinary"
    case .binaryView:
      return "BinaryView"
    case .utf8:
      return "Utf8"
    case .largeUtf8:
      return "LargeUtf8"
    case .utf8View:
      return "Utf8View"
    case .list(let elementType):
      return "List(\(elementType))"
    case .listView(let elementType):
      return "ListView(\(elementType))"
    case .fixedSizeList(let elementType, let size):
      return "FixedSizeList(\(elementType), \(size))"
    case .largeList(let elementType):
      return "LargeList(\(elementType))"
    case .largeListView(let elementType):
      return "LargeListView(\(elementType))"
    case .union(let mode, let fields):
      return "Union(\(mode), \(fields) fields)"
    case .dictionary(let id, let isOrdered, let keyType, let valueType):
      return "Dictionary(\(id), \(isOrdered), \(keyType), \(valueType))"
    case .decimal32(let precision, let scale):
      return "Decimal32(\(precision), \(scale))"
    case .decimal64(let precision, let scale):
      return "Decimal64(\(precision), \(scale))"
    case .decimal128(let precision, let scale):
      return "Decimal128(\(precision), \(scale))"
    case .decimal256(let precision, let scale):
      return "Decimal256(\(precision), \(scale))"
    case .map(let keyType, let valueType):
      return "Map(\(keyType), \(valueType))"
    case .runEndEncoded(let runEndsType, let valueType):
      return "RunEndEncoded(\(runEndsType), \(valueType))"
    }
  }
}

extension ArrowType {

  /// Returns true if the type is primitive: (numeric, temporal).
  @inlinable
  public var isPrimitive: Bool {
    self.isNumeric || self.isTemporal
  }

  /// Returns true if this type is numeric: (UInt*, Int*, Float*, Decimal*).
  @inlinable
  public var isNumeric: Bool {
    switch self {
    case .uint8,
      .uint16,
      .uint32,
      .uint64,
      .int8,
      .int16,
      .int32,
      .int64,
      .float16,
      .float32,
      .float64,
      .decimal32,
      .decimal64,
      .decimal128,
      .decimal256:
      return true
    default:
      return false
    }
  }

  /// Returns true if this type is temporal: (Date*, Time*, Duration, or Interval).
  @inlinable
  public var isTemporal: Bool {
    switch self {
    case .date32,
      .date64,
      .timestamp,
      .time32,
      .time64,
      .duration,
      .interval:
      return true
    default:
      return false
    }
  }

  /// Returns true if this type is floating: (Float*).
  @inlinable
  public var isFloating: Bool {
    switch self {
    case .float16, .float32, .float64:
      return true
    default:
      return false
    }
  }

  /// Returns true if this type is integer: (Int*, UInt*).
  @inlinable
  public var isInteger: Bool {
    self.isSignedInteger || self.isUnsignedInteger
  }

  /// Returns true if this type is signed integer: (Int*).
  @inlinable
  public var isSignedInteger: Bool {
    switch self {
    case .int8, .int16, .int32, .int64:
      return true
    default:
      return false
    }
  }

  /// Returns true if this type is unsigned integer: (UInt*).
  @inlinable
  public var isUnsignedInteger: Bool {
    switch self {
    case .uint8, .uint16, .uint32, .uint64:
      return true
    default:
      return false
    }
  }

  /// Returns true if this type is valid as a dictionary key.
  @inlinable
  public var isDictionaryKeyType: Bool {
    self.isInteger
  }

  /// Returns true if this type is valid for run-ends array in RunArray.
  @inlinable
  public var isRunEndsType: Bool {
    switch self {
    case .int16, .int32, .int64:
      return true
    default:
      return false
    }
  }

  /// Returns true if this type is nested.
  ///
  ///  Nested types are: (List, FixedSizeList, LargeList, ListView. LargeListView, Struct, Union, or Map,
  ///  or a dictionary of a nested type.
  @inlinable
  public var isNested: Bool {
    switch self {
    case .dictionary(_, _, _, let v):
      return v.isNested
    case .runEndEncoded(_, let v):
      return v.type.isNested
    case .list, .fixedSizeList, .largeList, .listView, .largeListView,
      .strct, .union, .map:
      return true
    default:
      return false
    }
  }

  /// Returns true if this type is a variable-length data type.
  ///
  /// https://arrow.apache.org/docs/format/Intro.html#variable-length-binary-and-string
  @inlinable
  public var isVariable: Bool {
    switch self {
    case .binary, .utf8, .largeBinary, .largeUtf8: true
    default: false
    }
  }

  @inlinable
  public var isBinaryView: Bool {
    switch self {
    case .binaryView, .utf8View: true
    default: false
    }
  }

  /// Returns true if this type is DataType::Null.
  @inlinable
  public var isNull: Bool {
    if case .null = self {
      return true
    } else {
      return false
    }
  }

  /// Compares the datatype with another, ignoring nested field names and metadata.
  public func equalsDataType(_ other: ArrowType) -> Bool {
    switch (self, other) {

    // List-like types
    case (.list(let a), .list(let b)),
      (.largeList(let a), .largeList(let b)),
      (.listView(let a), .listView(let b)),
      (.largeListView(let a), .largeListView(let b)):
      return a.isNullable == b.isNullable
        && a.type.equalsDataType(b.type)

    // FixedSizeList
    case (.fixedSizeList(let a, let aSize), .fixedSizeList(let b, let bSize)):
      return aSize == bSize && a.isNullable == b.isNullable
        && a.type.equalsDataType(b.type)

    // Struct
    case (.strct(let aFields), .strct(let bFields)):
      guard aFields.count == bFields.count else { return false }
      return zip(aFields, bFields).allSatisfy {
        $0.isNullable == $1.isNullable
          && $0.type.equalsDataType($1.type)
      }

    // Map
    case (.map(let aField, let aSorted), .map(let bField, let bSorted)):
      return aField.isNullable == bField.isNullable
        && aField.type.equalsDataType(bField.type) && aSorted == bSorted

    // Dictionary
    case (.dictionary(_, _, let aKey, let aValue), .dictionary(_, _, let bKey, let bValue)):
      // Ignoring dictionary id here.
      return aKey.equalsDataType(bKey) && aValue.equalsDataType(bValue)

    // RunEndEncoded
    case (
      .runEndEncoded(let aRunEnds, let aValues),
      .runEndEncoded(let bRunEnds, let bValues)
    ):
      return aRunEnds.isNullable == bRunEnds.isNullable
        && aRunEnds.type.equalsDataType(bRunEnds.type)
        && aValues.isNullable == bValues.isNullable
        && aValues.type.equalsDataType(bValues.type)

    // Union
    case (.union(let aFields, let aMode), .union(let bFields, let bMode)):
      guard aMode == bMode,
        aFields.count == bFields.count
      else { return false }

      return aFields.allSatisfy { aField in
        bFields.contains { bField in

          aField.typeId == bField.typeId
            && aField.field.isNullable == bField.field.isNullable
            && aField.field.type.equalsDataType(bField.field.type)
        }
      }

    // Default: strict equality
    default:
      return self == other
    }
  }

  /// Returns the byte width of this type if it is a primitive type.
  ///
  /// Returns `None` if not a primitive type
  @inlinable
  public var primitiveWidth: Int? {
    switch self {
    case .null: return nil
    case .boolean: return nil
    case .int8, .uint8: return 1
    case .int16, .uint16, .float16: return 2
    case .int32, .uint32, .float32: return 4
    case .int64, .uint64, .float64: return 8
    case .timestamp: return 8
    case .date32, .time32: return 4
    case .date64, .time64: return 8
    case .duration: return 8
    case .interval(.yearMonth): return 4
    case .interval(.dayTime): return 8
    case .interval(.monthDayNano): return 16
    case .decimal32: return 4
    case .decimal64: return 8
    case .decimal128: return 16
    case .decimal256: return 32
    case .utf8, .largeUtf8, .utf8View: return nil
    case .binary, .largeBinary, .binaryView: return nil
    case .fixedSizeBinary: return nil
    case .list, .listView, .largeList, .largeListView, .map: return nil
    case .fixedSizeList: return nil
    case .strct: return nil
    case .union: return nil
    case .dictionary: return nil
    case .runEndEncoded: return nil
    }
  }

  public func getStride() -> Int {
    switch self {
    case .int8:
      return MemoryLayout<Int8>.stride
    case .int16:
      return MemoryLayout<Int16>.stride
    case .int32:
      return MemoryLayout<Int32>.stride
    case .int64:
      return MemoryLayout<Int64>.stride
    case .uint8:
      return MemoryLayout<UInt8>.stride
    case .uint16:
      return MemoryLayout<UInt16>.stride
    case .uint32:
      return MemoryLayout<UInt32>.stride
    case .uint64:
      return MemoryLayout<UInt64>.stride
    case .float32:
      return MemoryLayout<Float>.stride
    case .float64:
      return MemoryLayout<Double>.stride
    case .boolean:
      return MemoryLayout<Bool>.stride
    case .date32:
      return MemoryLayout<Date32>.stride
    case .date64:
      return MemoryLayout<Date64>.stride
    case .time32:
      return MemoryLayout<Time32>.stride
    case .time64:
      return MemoryLayout<Time64>.stride
    case .timestamp:
      return MemoryLayout<Timestamp>.stride
    case .binary:
      return MemoryLayout<Int8>.stride
    case .utf8:
      return MemoryLayout<Int8>.stride
    case .strct, .list:
      return 0
    default:
      fatalError("Stride requested for unknown type: \(self)")
    }
  }

  /// Check  if `self` is a superset of `other`.
  ///
  /// If it is a nested type,  it will check if the nested type is a superset of the other nested type.
  /// Otherwise it will check to see if the DataType is equal to the other DataType.
  ///
  /// If the type is nested (List, Struct, etc.), this checks recursively
  /// whether the nested type is a superset of the other's nested type.
  /// Otherwise, it checks equality.
  public func contains(_ other: ArrowType) -> Bool {
    switch (self, other) {

    // ─────────────────────────────
    // List-like types
    case (.list(let f1), .list(let f2)),
      (.largeList(let f1), .largeList(let f2)),
      (.listView(let f1), .listView(let f2)),
      (.largeListView(let f1), .largeListView(let f2)):
      return f1.contains(other: f2)

    // FixedSizeList
    case (.fixedSizeList(let f1, let s1), .fixedSizeList(let f2, let s2)):
      return s1 == s2 && f1.contains(other: f2)

    // Map
    case (.map(let f1, let s1), .map(let f2, let s2)):
      return s1 == s2 && f1.contains(other: f2)

    // Struct
    case (.strct(let f1), .strct(let f2)):
      return f1.contains(f2)

    // Union
    case (.union(let f1, let s1), .union(let f2, let s2)):
      guard s1 == s2 else { return false }
      return f1.allSatisfy { unionFieldA in
        f2.contains { unionFieldB in
          unionFieldA.typeId == unionFieldB.typeId
            && unionFieldA.field.contains(other: unionFieldB.field)
        }
      }

    // Dictionary
    case (.dictionary(_, _, let k1, let v1), .dictionary(_, _, let k2, let v2)):
      return k1.contains(k2) && v1.contains(v2)

    // Base case: equality
    default:
      return self == other
    }
  }

  /// Create a `List` with elements of the specified type and nullability.
  public init(listFieldWith dataType: ArrowType, isNullable: Bool) {
    self = .list(ArrowField(listFieldWith: dataType, isNullable: isNullable))
  }

  /// Create a `LargeList` with elements of the specified type and nullability.
  public init(largeListFieldWith dataType: ArrowType, isNullable: Bool) {
    self = .largeList(
      (ArrowField(listFieldWith: dataType, isNullable: isNullable)))
  }

  /// Create a `FixedSizeList` with elements of the specified type, size  and nullability.
  public init(
    fixedListFieldWith dataType: ArrowType, size: Int32, isNullable: Bool
  ) {
    self = .fixedSizeList(
      ArrowField(listFieldWith: dataType, isNullable: isNullable), size)
  }
}

extension ArrowType {

  /// The C-interface format string.
  ///
  /// https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
  public var cDataFormatId: String {
    get throws(ArrowError) {
      switch self {
      case .int8:
        return "c"
      case .int16:
        return "s"
      case .int32:
        return "i"
      case .int64:
        return "l"
      case .uint8:
        return "C"
      case .uint16:
        return "S"
      case .uint32:
        return "I"
      case .uint64:
        return "L"
      case .float32:
        return "f"
      case .float64:
        return "g"
      case .boolean:
        return "b"
      case .date32:
        return "tdD"
      case .date64:
        return "tdm"
      case .time32(let unit):
        switch unit {
        case .millisecond:
          return "ttm"
        case .second:
          return "tts"
        default:
          throw .init(.invalid("\(unit) invalid for Time32."))
        }
      case .time64(let unit):
        switch unit {
        case .microsecond:
          return "ttu"
        case .nanosecond:
          return "ttn"
        default:
          throw .init(.invalid("\(unit) invalid for Time64."))
        }
      case .timestamp(let unit, let timezone):
        let unitChar: Character =
          switch unit {
          case .second: "s"
          case .millisecond: "m"
          case .microsecond: "u"
          case .nanosecond: "n"
          }

        if let timezone {
          return "ts\(unitChar):\(timezone)"
        } else {
          return "ts\(unitChar)"
        }
      case .binary:
        return "z"
      case .utf8:
        return "u"
      case .strct(let fields):
        var format = "+s"
        for field in fields {
          format += try field.type.cDataFormatId
        }
        return format
      case .list(let field):
        return "+l" + (try field.type.cDataFormatId)
      default:
        throw .init(.notImplemented("cData not implmented for \(self)."))
      }
    }
  }

  public static func fromCDataFormatId(
    _ from: String
  ) throws(ArrowError) -> ArrowType {
    if from == "c" {
      return .int8
    } else if from == "s" {
      return .int16
    } else if from == "i" {
      return .int32
    } else if from == "l" {
      return .int64
    } else if from == "C" {
      return .uint8
    } else if from == "S" {
      return .uint16
    } else if from == "I" {
      return .uint32
    } else if from == "L" {
      return .uint64
    } else if from == "f" {
      return .float32
    } else if from == "g" {
      return .float64
    } else if from == "b" {
      return .boolean
    } else if from == "tdD" {
      return .date32
    } else if from == "tdm" {
      return .date64
    } else if from == "tts" {
      return .time32(.second)
    } else if from == "ttm" {
      return .time32(.millisecond)
    } else if from == "ttu" {
      return .time64(.microsecond)
    } else if from == "ttn" {
      return .time64(.nanosecond)
    } else if from.starts(with: "ts") {
      let components = from.split(separator: ":", maxSplits: 1)
      guard let unitPart = components.first, unitPart.count == 3 else {
        throw .init(
          .invalid(
            "Invalid timestamp format '\(from)'. Expected format 'ts[s|m|u|n][:timezone]'"
          ))
      }
      let unitChar = unitPart.suffix(1)
      let unit: TimeUnit =
        switch unitChar {
        case "s": .second
        case "m": .millisecond
        case "u": .microsecond
        case "n": .nanosecond
        default:
          throw .init(
            .invalid(
              "Unrecognized timestamp unit '\(unitChar)'. Expected 's', 'm', 'u', or 'n'."
            ))
        }
      let timezone = components.count > 1 ? String(components[1]) : nil
      return .timestamp(unit, timezone)
    } else if from == "z" {
      return .binary
    } else if from == "u" {
      return .utf8
    }
    throw .init(.notImplemented("cData not implmented for \(self)."))
  }
}
