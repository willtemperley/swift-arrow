import Arrow
import Benchmark
import Foundation

// MARK: - Test data setup

private let count = 10_000_000

private struct SendablePointer<T>: @unchecked Sendable {
  let pointer: UnsafePointer<T>
}

private let storage: SendablePointer<UInt64> = {
  let p = UnsafeMutablePointer<UInt64>.allocate(capacity: count)
  for i in 0..<count {
    p[i] = UInt64(i)
  }
  return SendablePointer(pointer: UnsafePointer(p))
}()

private let buffer = FixedWidthBuffer<UInt64>(
  length: count * MemoryLayout<UInt64>.stride,
  capacity: count * MemoryLayout<UInt64>.stride,
  valueCount: count,
  ownsMemory: false,
  buffer: storage.pointer
)

private let nullBuffer = AllValidNullBuffer(valueCount: count)

// MARK: - Fixtures

private let existentialArray = ArrowArrayNumericExistential<UInt64>(
  length: count,
  nullBuffer: nullBuffer,
  valueBuffer: buffer
)

private let enumArray = ArrowArrayNumeric<UInt64>(
  length: count,
  nullBuffer: nullBuffer,
  valueBuffer: .allocated(buffer)
)

// MARK: - Raw baseline (no buffer abstraction)

struct ArrowArrayNumericRaw<ItemType: Numeric & BitwiseCopyable>:
  @unchecked Sendable
{
  let offset: Int
  let length: Int
  let nullBuffer: NullBuffer
  let pointer: UnsafePointer<ItemType>

  subscript(index: Int) -> ItemType? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    return pointer[offsetIndex]
  }
}

private let rawArray = ArrowArrayNumericRaw<UInt64>(
  offset: 0,
  length: count,
  nullBuffer: nullBuffer,
  pointer: storage.pointer
)

private struct MmapArrayView: @unchecked Sendable {
  let offset: Int
  let length: Int
  let nullBuffer: NullBuffer
  let buffer: FixedWidthBufferIPC2<UInt64>

  subscript(index: Int) -> UInt64? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    return buffer[offsetIndex]
  }
}

private let mmapArray = MmapArrayView(
  offset: 0,
  length: count,
  nullBuffer: nullBuffer,
  buffer: mmapBuffer
)

private let swiftArray: [UInt64?] = (0..<UInt64(count)).map { Optional($0) }

//private let ipcBuffer: FixedWidthBufferIPC<UInt64> = {
//  let data = Data(
//    bytes: storage.pointer, count: count * MemoryLayout<UInt64>.stride)
//  let fdb = FileDataBuffer(data: data, range: 0..<data.count)
//  return FixedWidthBufferIPC(buffer: fdb)
//}()

private let mmapBuffer: FixedWidthBufferIPC2<UInt64> = {
  let byteCount = count * MemoryLayout<UInt64>.stride
  let path =
    NSTemporaryDirectory()
    + "arrow_bench_\(ProcessInfo.processInfo.processIdentifier).bin"
  let data = Data(bytes: storage.pointer, count: byteCount)
  FileManager.default.createFile(atPath: path, contents: data)
  let file = try! MappedFile(path: path)
  let fdb = FileDataBuffer2(file: file, range: 0..<byteCount)
  return FixedWidthBufferIPC2(buffer: fdb)
}()

//private let enumArrayIPC = ArrowArrayNumeric<UInt64>(
//  length: count,
//  nullBuffer: nullBuffer,
//  valueBuffer: .ipc(ipcBuffer)
//)

//private let existentialArrayIPC = ArrowArrayNumericExistential<UInt64>(
//  length: count,
//  nullBuffer: nullBuffer,
//  valueBuffer: ipcBuffer
//)

// MARK: - Benchmarks

let benchmarks: @Sendable () -> Void = {

  Benchmark(
    "Subscript — Existential dispatch",
    configuration: .init(
      metrics: [.cpuTotal, .throughput, .peakMemoryResident],
      maxDuration: .seconds(10)
    )
  ) { benchmark in
    var sum: UInt64 = 0
    for i in 0..<count {
      sum &+= existentialArray[i] ?? 0
    }
    blackHole(sum)
  }

  //  Benchmark(
  //    "Subscript — Existential dispatch (IPC)",
  //    configuration: .init(
  //      metrics: [.cpuTotal, .throughput, .peakMemoryResident],
  //      maxDuration: .seconds(10)
  //    )
  //  ) { benchmark in
  //    var sum: UInt64 = 0
  //    for i in 0..<count {
  //      sum &+= existentialArrayIPC[i] ?? 0
  //    }
  //    blackHole(sum)
  //  }

  Benchmark(
    "Subscript — Enum dispatch",
    configuration: .init(
      metrics: [.cpuTotal, .throughput, .peakMemoryResident],
      maxDuration: .seconds(10)
    )
  ) { benchmark in
    var sum: UInt64 = 0
    for i in 0..<count {
      sum &+= enumArray[i] ?? 0
    }
    blackHole(sum)
  }

  //  Benchmark(
  //    "Subscript — Enum dispatch (IPC)",
  //    configuration: .init(
  //      metrics: [.cpuTotal, .throughput, .peakMemoryResident],
  //      maxDuration: .seconds(10)
  //    )
  //  ) { benchmark in
  //    var sum: UInt64 = 0
  //    for i in 0..<count {
  //      sum &+= enumArrayIPC[i] ?? 0
  //    }
  //    blackHole(sum)
  //  }

  Benchmark(
    "Subscript — mmap (IPC)",
    configuration: .init(
      metrics: [.cpuTotal, .throughput, .peakMemoryResident],
      maxDuration: .seconds(10)
    )
  ) { benchmark in
    var sum: UInt64 = 0
    for i in 0..<count {
      sum &+= mmapArray[i] ?? 0
    }
    blackHole(sum)
  }

  Benchmark(
    "Subscript — Raw pointer",
    configuration: .init(
      metrics: [.cpuTotal, .throughput, .peakMemoryResident],
      maxDuration: .seconds(10)
    )
  ) { benchmark in
    var sum: UInt64 = 0
    for i in 0..<count {
      sum &+= rawArray[i] ?? 0
    }
    blackHole(sum)
  }

  Benchmark(
    "Subscript — Swift Array",
    configuration: .init(
      metrics: [.cpuTotal, .throughput, .peakMemoryResident],
      maxDuration: .seconds(10)
    )
  ) { benchmark in
    var sum: UInt64 = 0
    for i in 0..<count {
      sum &+= swiftArray[i] ?? 0
    }
    blackHole(sum)
  }

  Benchmark(
    "Subscript — Raw pointer (no null check)",
    configuration: .init(
      metrics: [.cpuTotal, .throughput, .peakMemoryResident],
      maxDuration: .seconds(10)
    )
  ) { benchmark in
    let p = storage.pointer
    var sum: UInt64 = 0
    for i in 0..<count {
      sum &+= p[i]
    }
    blackHole(sum)
  }

}
