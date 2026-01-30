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
import Foundation
import Subprocess

/// Deserialize Arrow IPC and re-serialize to IPC.
/// - Parameter ipcData: The IPC data.
/// - Throws: An error if either the subprocess call fails, or the called script fails.
/// - Returns: The
func pyArrowRoundTrip(ipcData: Data) async throws -> Data {

  // Validate with PyArrow
  let result = try await run(
    .path("/opt/anaconda3/bin/python3"),
    arguments: [
      "-c",
      """
      import sys

      import pyarrow as pa
      from io import BytesIO

      input_data = sys.stdin.buffer.read()
      buffer = BytesIO(input_data)

      reader = pa.ipc.open_file(buffer)

      writer = pa.ipc.new_file(sys.stdout.buffer, reader.schema)
      for i in range(reader.num_record_batches):
          batch = reader.get_batch(i)
          writer.write_batch(batch)
      writer.close()
      """,
    ],
    input: .data(ipcData),
    output: .data(limit: 100_000_000),
    error: .string(limit: 4096)
  )

  guard result.terminationStatus.isSuccess else {
    let errorMessage = result.standardError ?? "Unknown error"
    throw ArrowError(
      .invalid("Unable to validate IPC data with PyArrow: \(errorMessage)."))
  }

  return result.standardOutput
}
