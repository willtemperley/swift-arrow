// Copyright 2025 Columnar-Swift contributors
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

typealias FFooter = org_apache_arrow_flatbuf_Footer

typealias FMessageHeader = org_apache_arrow_flatbuf_MessageHeader
typealias FMessage = org_apache_arrow_flatbuf_Message

typealias FBlock = org_apache_arrow_flatbuf_Block
typealias FField = org_apache_arrow_flatbuf_Field
typealias FSchema = org_apache_arrow_flatbuf_Schema
typealias FBuffer = org_apache_arrow_flatbuf_Buffer
typealias FFieldNode = org_apache_arrow_flatbuf_FieldNode
typealias FKeyValue = org_apache_arrow_flatbuf_KeyValue

// MARK: Record batches.
typealias FRecordBatch = org_apache_arrow_flatbuf_RecordBatch
typealias FDictionaryBatch = org_apache_arrow_flatbuf_DictionaryBatch
typealias FDictionaryEncoding = org_apache_arrow_flatbuf_DictionaryEncoding

// MARK: Top level type.
typealias FType = org_apache_arrow_flatbuf_Type_

// MARK: Primitive types.
typealias FFloatingPoint = org_apache_arrow_flatbuf_FloatingPoint
typealias FInt = org_apache_arrow_flatbuf_Int
typealias FBool = org_apache_arrow_flatbuf_Bool
typealias FDate = org_apache_arrow_flatbuf_Date
typealias FTime = org_apache_arrow_flatbuf_Time
typealias FDuration = org_apache_arrow_flatbuf_Duration
typealias FTimestamp = org_apache_arrow_flatbuf_Timestamp
typealias FTimeUnit = org_apache_arrow_flatbuf_TimeUnit

// MARK: Nested types.
typealias FStruct = org_apache_arrow_flatbuf_Struct_
typealias FList = org_apache_arrow_flatbuf_List
typealias FLargeList = org_apache_arrow_flatbuf_LargeList
typealias FMap = org_apache_arrow_flatbuf_Map
typealias FFixedSizeList = org_apache_arrow_flatbuf_FixedSizeList

// MARK: Binary types.
typealias FBinary = org_apache_arrow_flatbuf_Binary
typealias FLargeBinary = org_apache_arrow_flatbuf_LargeBinary
typealias FBinaryView = org_apache_arrow_flatbuf_BinaryView
typealias FUtf8 = org_apache_arrow_flatbuf_Utf8
typealias FUtf8View = org_apache_arrow_flatbuf_Utf8View
typealias FLargeUtf8 = org_apache_arrow_flatbuf_LargeUtf8
typealias FFixedSizeBinary = org_apache_arrow_flatbuf_FixedSizeBinary
