// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
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

  printf("opening %s\n", name.c_str());
  mkdir(name.c_str(), 0755);

  for (int i = 0; i < NUM_PARTS; i++) {
    std::string file_name = name + "/" + mapping[i];
    int fd = open(file_name.c_str(), O_RDONLY);
    if (fd > 0) {
      while (1) {
        uint32_t key_len, value_len;

        if (read(fd, &key_len, 4) != 4) {
          close(fd);
          break;
        }
        if (read(fd, key_buffer, key_len) != key_len) {
          printf("Unexpected data\n");
          close(fd);
          break;
        }
        if (read(fd, &value_len, 4) != 4) {
          printf("Unexpected data\n");
          close(fd);
          break;
        }
        if (read(fd, value_buffer, value_len) != value_len) {
          printf("Unexpected data\n");
          close(fd);
          break;
        }
        //printf("Recover %s %s\n", key_buffer, value_buffer);
        std::string key(key_buffer);
        auto search = engine_race->data[i].find(key);
        if (search != engine_race->data[i].end()) {
          engine_race->data[i].erase(search);
        }
        engine_race->data[i].insert(
            std::make_pair(key, std::string(value_buffer)));
      }
    }
  }

  for (int i = 0; i < NUM_PARTS; i++) {
    std::string file_name = name + "/" + mapping[i];
    int fd = open(file_name.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_DIRECT, 0755);
    if (fd < 0) {
      perror("open");
      printf("file %s unable to open\n", file_name.c_str());
      engine_race->fds[i] = -1;
      return kIOError;
    } else {
      engine_race->fds[i] = fd;
    }
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

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
  const char *key_data = key.data();
  int part = key_data[0];
  char *write_buffer;
  uint32_t key_len = key.size() + 1;
  uint32_t value_len = value.size() + 1;
  size_t total_len = 4 + key_len + 4 + value_len;
  if (posix_memalign((void**)&write_buffer, page_size, total_len) < 0) {
    perror("posix_memalign");
    return kIOError;
  }
  *((uint32_t *)write_buffer) = key_len;
  memcpy(&write_buffer[4], key.data(), key_len);
  total_len = (total_len + page_size - 1) & (-page_size);
  *((uint32_t *)&write_buffer[4 + key_len]) = total_len - 4 - key_len - 4;
  memcpy(&write_buffer[4 + key_len + 4], value.data(), value_len);
  locks[part].lock();
  size_t written = 0;
  while (written < total_len) {
    int res = write(fds[part], &write_buffer[written], total_len - written);
    if (res < 0) {
      perror("write");
      locks[part].unlock();
      free(write_buffer);
      return kIOError;
    } else {
      written += res;
    }
  }
  //fsync(fds[part]);
  auto search = data[part].find(key.ToString());
  if (search != data[part].end()) {
    data[part].erase(search);
  }
  data[part].insert(
      std::make_pair(key.ToString(), value.ToString()));
  locks[part].unlock();
  free(write_buffer);
  return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString &key, std::string *value) {
  const char *key_data = key.data();
  int part = key_data[0];
  locks[part].lock();
  auto search = data[part].find(key.ToString());
  if (search != data[part].end()) {
    *value = search->second;
    locks[part].unlock();
    return kSucc;
  } else {
    locks[part].unlock();
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
