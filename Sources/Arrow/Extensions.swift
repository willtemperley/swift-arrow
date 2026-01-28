// Copyright 2026 The Columnar Swift Contributors
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

extension ArrowField {
  
  /// Return the GeoArrow extension type if set, else returns nil.
  public var geoArrowType: GeoArrowType? {
    metadata["ARROW:extension:name"].flatMap { GeoArrowType(rawValue: $0) }
  }
}

/// The GeoArrow types.
///
/// Use these in conjunction with a column's `ArrowType` to interpret a geometry column. There are
/// multiple possible memory layouts for each type.
public enum GeoArrowType: String {
  case point = "geoarrow.point"
  case linestring = "geoarrow.linestring"
  case polygon = "geoarrow.polygon"
  case multipoint = "geoarrow.multipoint"
  case multilinestring = "geoarrow.multilinestring"
  case multipolygon = "geoarrow.multipolygon"
  case geometry = "geoarrow.geometry"
  case geometrycollection = "geoarrow.geometrycollection"
  case box = "geoarrow.box"
  case wkb = "geoarrow.wkb"
  case wkt = "geoarrow.wkt"
}
