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

public enum FixedWidthBufferStorage<T>: FixedWidthBufferProtocol
where T: BitwiseCopyable {

  case allocated(FixedWidthBuffer<T>)
  case ipc(FixedWidthBufferIPC2<T>)

  public var length: Int {
    switch self {
    case .allocated(let b): return b.length
    case .ipc(let b): return b.length
    }
  }

  public subscript(index: Int) -> T {
    switch self {
    case .allocated(let b): return b[index]
    case .ipc(let b): return b[index]
    }
  }

  public func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R {
    switch self {
    case .allocated(let b): return try b.withUnsafeBytes(body)
    case .ipc(let b): return try b.withUnsafeBytes(body)
    }
  }
}
