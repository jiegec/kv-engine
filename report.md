# Key-Value 存储引擎实验报告

## 实验目标

实现一个简单的 KV 存储引擎，支持读写和可选的范围搜索功能，并且需要保证写入的一致性。这里把写入的一致性理解为一旦Write函数返回，即使机器立即断电重启也能保证数据完好。

## 实验成果

实现了一个满足以上要求的 KV 存储引擎，并且包括范围搜索功能，通过了测试和额外编写的范围搜索测试，在牺牲一定性能的情况下保证了比较强的写入的一致性。

## 实现方法

### 数据结构的选择

最后采用的是 `std::map` ，曾经尝试使用哈希表，但是经过性能测试后发现，瓶颈不在数据结构中，所以为了支持范围搜索又切换回了树的结构，并且 STL 的实现在开启了优化以后也是足够快的。

### 写入持久化和一致性

为了保证 Write 成功返回后就能保证数据正确写入，我采用了 Write ahead log 配合 Direct IO 的机制。为了保证 Write 的原子性，我添加了读写锁，保证了多线程的执行的正确性。一些具体实现细节如下：

#### 根据数据第一个字节进行分片

考虑到采用的数据结构是 `std::map` ，不能在数据结构内部加锁，所以只能在外部加锁。为了让锁的粒度缩小，我对 key 的第一个字节进行了分割，每一个取值分配到对应的分片中，这样就把一把大锁分配到了256个锁上，文件也相应拆分为256个文件。

```c++
const char *mapping[NUM_PARTS] = {
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0a", "0b",
    "0c", "0d", "0e", "0f", "10", "11", "12", "13", "14", "15", "16", "17",
    "18", "19", "1a", "1b", "1c", "1d", "1e", "1f", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "2a", "2b", "2c", "2d", "2e", "2f",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3a", "3b",
    "3c", "3d", "3e", "3f", "40", "41", "42", "43", "44", "45", "46", "47",
    "48", "49", "4a", "4b", "4c", "4d", "4e", "4f", "50", "51", "52", "53",
    "54", "55", "56", "57", "58", "59", "5a", "5b", "5c", "5d", "5e", "5f",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6a", "6b",
    "6c", "6d", "6e", "6f", "70", "71", "72", "73", "74", "75", "76", "77",
    "78", "79", "7a", "7b", "7c", "7d", "7e", "7f", "80", "81", "82", "83",
    "84", "85", "86", "87", "88", "89", "8a", "8b", "8c", "8d", "8e", "8f",
    "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9a", "9b",
    "9c", "9d", "9e", "9f", "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7",
    "a8", "a9", "aa", "ab", "ac", "ad", "ae", "af", "b0", "b1", "b2", "b3",
    "b4", "b5", "b6", "b7", "b8", "b9", "ba", "bb", "bc", "bd", "be", "bf",
    "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "ca", "cb",
    "cc", "cd", "ce", "cf", "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7",
    "d8", "d9", "da", "db", "dc", "dd", "de", "df", "e0", "e1", "e2", "e3",
    "e4", "e5", "e6", "e7", "e8", "e9", "ea", "eb", "ec", "ed", "ee", "ef",
    "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "fa", "fb",
    "fc", "fd", "fe", "ff"};
```

#### 读写锁的使用

在写入和读取的时候都添加了相应的锁：

```c++
RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
  pthread_rwlock_wrlock(&locks[part]);
  // ...
  pthread_rwlock_unlock(&locks[part]);
}

RetCode EngineRace::Read(const PolarString &key, std::string *value) {
  pthread_rwlock_rdlock(&locks[part]);
  // ...
  pthread_rwlock_unlock(&locks[part]);
}
```

#### Write Ahead Log 的实现

每次更新的时候，都先通过 DirectIO 写入到文件中，WAL 的格式大概如下：

| Key 长度（4字节）  |
| ------------------ |
| Key 内容           |
| Data 长度（4字节） |
| Data 内容          |

构造 WAL 格式的数据：

```cpp
uint32_t key_len = key.size();
uint32_t value_len = value.size();
size_t total_len = 4 + key_len + 4 + 4 + value_len;
total_len = (total_len + page_size - 1) & (-page_size);
if (write_buffer == NULL) {
  int max_len =
    (4 + 4 + 4 + 1024 + 1024 * 1024 * 4 + page_size - 1) & (-page_size);
  if (posix_memalign((void **)&write_buffer, page_size, max_len) < 0) {
    perror("posix_memalign");
    return kIOError;
  }
}

*((uint32_t *)write_buffer) = key_len;
memcpy(&write_buffer[4], key.data(), key_len);
*((uint32_t *)&write_buffer[4 + key_len]) =
  total_len - 4 - key_len - 4 - 4 - value_len;
*((uint32_t *)&write_buffer[4 + key_len + 4]) = value_len;
memcpy(&write_buffer[4 + key_len + 4 + 4], value.data(), value_len);
```

由于 DirectIO 要求读写对齐到页，所以这里做了特殊的处理：分配的内存空间是页对齐的，这里做了一些小的优化，提前分配好对齐的内存，并且这个内存的指针是放在 TLS （Thread Local Storage）中的，这样免去了加锁的麻烦。接着就是写入文件并且写入数据结构：

```cpp
size_t written = 0;
while (written < total_len) {
  int res = write(fds[part], &write_buffer[written], total_len - written);
  if (res >= 0) {
    written += res;
  } else {
    perror("write");
    pthread_rwlock_unlock(&locks[part]);
    free(write_buffer);
    return kIOError;
  }
}
auto search = data[part].find(key_string);
if (search != data[part].end()) {
  data[part].erase(search);
}
data[part].insert(std::make_pair(key_string, value_string));
```

曾经也在这里尝试过 `mmap+msync`或 `write+fsync` 的组合，但性能都不如 Direct IO 快。所以最后还是选择了 DirectIO 。

#### 数据恢复的实现

在初始化 EngineRace 的时候，会先读取 Write Ahead Log 进行恢复，重现之前做过的操作。这里采用了 `mmap` 后读取的方式，不断寻找数据，找不到正确的数据就退出：

```cpp
while (mapped < end) {
  uint32_t key_len, value_len, skip_len;

  if (mapped + 4 >= end) {
    break;
  }
  key_len = *(uint32_t *)mapped;
  mapped += 4;

  if (mapped + key_len >= end) {
    break;
  }
  memcpy(key_buffer, mapped, key_len);
  mapped += key_len;

  if (mapped + 4 >= end) {
    break;
  }
  skip_len = *(uint32_t *)mapped;
  mapped += 4;

  if (mapped + 4 >= end) {
    break;
  }
  value_len = *(uint32_t *)mapped;
  mapped += 4;

  if (mapped + value_len >= end) {
    break;
  }
  memcpy(value_buffer, mapped, value_len);
  mapped += value_len;
  mapped += skip_len;

  std::string key(key_buffer, key_len);
  std::string value(value_buffer, value_len);
  auto search = engine_race->data[i].find(key);
  if (search != engine_race->data[i].end()) {
    engine_race->data[i].erase(search);
  }
  engine_race->data[i].insert(std::make_pair(key, value));
  count++;
}
```

这样就保证了 Write Consistency，得以通过 Crash Test。

## 测试结果

### 功能测试

提供的三个测试程序都能正确通过：

```
======================= range thread test pass :) ======================
jiegec@vision2 /d/j/k/test> ./run_test.sh
======================= single thread test ============================
recovering ./data/test-3970804384279905
opening ./data/test-3970804384279905
open engine_path: ./data/test-3970804384279905
recovering ./data/test-3970804384279905
replayed 15003 actions
opening ./data/test-3970804384279905
======================= single thread test pass :) ======================
--------------------------------------
======================= multi thread test ============================
recovering ./data/test-3970822713410589
opening ./data/test-3970822713410589
open engine_path: ./data/test-3970822713410589
======================= multi thread test pass :) ======================
--------------------------------------
======================= crash test ============================
open engine_path: ./data/test-3971099929184937
recovering ./data/test-3971099929184937
opening ./data/test-3971099929184937
recovering ./data/test-3971099929184937
replayed 10002 actions
opening ./data/test-3971099929184937
======================= crash test pass :) ======================
```

### 性能测试

在实验平台 `Intel Xeon E5-2670 + HDD 5890rpm` 上， 对不同参数进行了测试 `./bench 线程数 读比例 分布`，数据为 throughput (op/s)：

| 线程数\（读比例，分布） | 99,0  | 99,1  | 90,0 | 90,1 | 50,0 | 50,1 |
| ----------------------- | ----- | ----- | ---- | ---- | ---- | ---- |
| 1                       | 43868 | 44479 | 5524 | 7116 | 1110 | 1320 |
| 2                       | 71473 | 64219 | 4964 | 6853 | 1060 | 1316 |
| 3                       | 65820 | 76572 | 5357 | 6833 | 1087 | 1296 |
| 4                       | 62415 | 79861 | 5452 | 6437 | N/A  | N/A  |
| 8                       | 60357 | 74840 | 5396 | 6636 | N/A  | N/A  |

从数据中可以看到很明显的趋势，随着读比例下降，由于写入的冲突增加，性能有了明显的下降。另一方面。多线程同时写入在冲突少的时候对性能有提高，但随着冲突增加和读写锁的开销增加，性能会大致稳定到一个值。而且由于测试使用的机器 CPU 主频和硬盘都不是很好，所以数据比较难看，在一个较好一些的平台中预计会有十数倍速度的提升，并且得到更多数据。

另外，当数据分布为 1 的时候，性能有一定的提升，说明输入数据的分布也会影响到键值存储引擎的性能。曾经测试过 DirectIO 以外的方案，如 fsync 和 msync ，但都不尽如人意。但如果不用这些，Crash Consistency 我认为是不能保证的。

## 思考问题

如何验证或者测试 Key Value 存储引擎的 Crash Consistency？

一种方法是拿真机来运行，断电，重启，测试，但是成本会比较高，好处是模拟了真实环境。第二种方法则是用一些虚拟机，如QEMU、VirtualBox等强制关机来模拟断电，但这些虚拟出来的磁盘，它一些操作的语义可能和真实情况不完全一样。在操作系统内用 `kill -9` 只能说是比较粗略的模拟方法了。