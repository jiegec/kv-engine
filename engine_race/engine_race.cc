// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdint.h>

namespace polar_race {

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

RetCode Engine::Open(const std::string &name, Engine **eptr) {
  return EngineRace::Open(name, eptr);
}

Engine::~Engine() {}

/*
 * Complete the functions below to implement you own engine
 */

// structure of write ahead log:
// 4-byte length: key length
// key data
// 4-byte length: value length
// value data
char key_buffer[2048];
char value_buffer[8 * 1024 * 1024];

// 1. Open engine
RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
  *eptr = NULL;
  EngineRace *engine_race = new EngineRace(name);

  printf("recovering %s\n", name.c_str());
  mkdir(name.c_str(), 0755);
  int count = 0;

  for (int i = 0; i < NUM_PARTS; i++) {
    std::string file_name = name + "/" + mapping[i];
    int fd = open(file_name.c_str(), O_RDONLY);
    if (fd > 0) {
      struct stat s;
      if (fstat(fd, &s) < 0) {
        perror("fstat");
        continue;
      }
      if (s.st_size == 0) {
        continue;
      }
      uint8_t *mapped = (uint8_t *) mmap(0, s.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
      uint8_t *orig_mapped = mapped;
      uint8_t *end = mapped + s.st_size;
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
        engine_race->data[i].insert(
            std::make_pair(key, value));
        count ++;
      }

      munmap(orig_mapped, s.st_size);
      close(fd);
    }
  }

  if (count) {
    printf("replayed %d actions\n", count);
  }

  printf("opening %s\n", name.c_str());
  for (int i = 0; i < NUM_PARTS; i++) {
    std::string file_name = name + "/" + mapping[i];
    int fd = open(file_name.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_DIRECT, 0755);
    if (fd < 0) {
      perror("open");
      printf("file %s is unable to open, check fd limit\n", file_name.c_str());
      engine_race->fds[i] = -1;
      return kIOError;
    } else {
      engine_race->fds[i] = fd;
    }

    engine_race->locks[i] = PTHREAD_RWLOCK_INITIALIZER;
  }

  engine_race->page_size = getpagesize();

  *eptr = engine_race;
  return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
  for (int i = 0; i < NUM_PARTS; i++) {
    if (fds[i] > 0) {
      close(fds[i]);
    }
  }
}

thread_local char *write_buffer = NULL;

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
  uint8_t part = key[0];
  uint32_t key_len = key.size();
  uint32_t value_len = value.size();
  size_t total_len = 4 + key_len + 4 + 4 + value_len;
  total_len = (total_len + page_size - 1) & (-page_size);
  if (write_buffer == NULL) {
    int max_len = (4 + 4 + 4 + sizeof(key_buffer) + sizeof(value_buffer) + page_size - 1) & (-page_size);
    if (posix_memalign((void**)&write_buffer, page_size, max_len) < 0) {
      perror("posix_memalign");
      return kIOError;
    }
  }

  *((uint32_t *)write_buffer) = key_len;
  memcpy(&write_buffer[4], key.data(), key_len);
  *((uint32_t *)&write_buffer[4 + key_len]) = total_len - 4 - key_len - 4 - 4 - value_len;
  *((uint32_t *)&write_buffer[4 + key_len + 4]) = value_len;
  memcpy(&write_buffer[4 + key_len + 4 + 4], value.data(), value_len);

  std::string key_string = key.ToString();
  std::string value_string = value.ToString();
  //locks[part].lock();
  pthread_rwlock_wrlock(&locks[part]);
  size_t written = 0;
  while (written < total_len) {
    int res = write(fds[part], &write_buffer[written], total_len - written);
    if (res >= 0) {
      written += res;
    } else {
      perror("write");
      //locks[part].unlock();
      pthread_rwlock_unlock(&locks[part]);
      free(write_buffer);
      return kIOError;
    }
  }
  //fsync(fds[part]);
  auto search = data[part].find(key_string);
  if (search != data[part].end()) {
    data[part].erase(search);
  }
  data[part].insert(
      std::make_pair(key_string, value_string));
  //locks[part].unlock();
  pthread_rwlock_unlock(&locks[part]);
  return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString &key, std::string *value) {
  uint8_t part = key[0];
  pthread_rwlock_rdlock(&locks[part]);
  //locks[part].lock();
  auto search = data[part].find(key.ToString());
  if (search != data[part].end()) {
    *value = search->second;
    //locks[part].unlock();
    pthread_rwlock_unlock(&locks[part]);
    return kSucc;
  } else {
    //locks[part].unlock();
    pthread_rwlock_unlock(&locks[part]);
    return kNotFound;
  }
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                          Visitor &visitor) {
  return kSucc;
}

} // namespace polar_race
