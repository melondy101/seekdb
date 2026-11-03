/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_CSV_READER_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_CSV_READER_H_

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace sql
{
// Streaming CSV reader used by the file-as-database feature.
// Records are read lazily from a local file; the whole file is never materialized.
// Fields returned by next_record() point into an internal buffer which is valid
// until the next call of next_record().
class ObFileCsvReader
{
public:
  static const int64_t READ_BUF_SIZE = 64 * 1024;
  static const int64_t MAX_RECORD_SIZE = 4 * 1024 * 1024;
public:
  ObFileCsvReader()
    : fp_(NULL), read_buf_(NULL), read_pos_(0), read_end_(0), rec_buf_(NULL), rec_len_(0),
      delim_(','), quote_('"'), escape_('\0'), record_cnt_(0)
  {}
  ~ObFileCsvReader() { close(); }
  int open(const common::ObString &path)
  {
    int ret = OB_SUCCESS;
    close();
    if (OB_UNLIKELY(path.empty())) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "empty file path", K(ret));
    } else if (NULL == (read_buf_ = static_cast<char *>(::malloc(READ_BUF_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (NULL == (rec_buf_ = static_cast<char *>(::malloc(MAX_RECORD_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (NULL == (fp_ = ::fopen(path.ptr(), "rb"))) {
      ret = OB_FILE_NOT_EXIST;
      COMMON_LOG(WARN, "failed to open file", K(ret), K(path));
    } else {
      read_pos_ = 0;
      read_end_ = 0;
      rec_len_ = 0;
      record_cnt_ = 0;
      // skip utf-8 BOM if present
      if (OB_SUCC(fill_buffer()) && read_end_ >= 3 &&
          0xEF == static_cast<unsigned char>(read_buf_[0]) &&
          0xBB == static_cast<unsigned char>(read_buf_[1]) &&
          0xBF == static_cast<unsigned char>(read_buf_[2])) {
        read_pos_ = 3;
      }
    }
    if (OB_FAIL(ret)) {
      close();
    }
    return ret;
  }
  void close()
  {
    if (NULL != fp_) {
      ::fclose(fp_);
      fp_ = NULL;
    }
    if (NULL != read_buf_) {
      ::free(read_buf_);
      read_buf_ = NULL;
    }
    if (NULL != rec_buf_) {
      ::free(rec_buf_);
      rec_buf_ = NULL;
    }
    read_pos_ = 0;
    read_end_ = 0;
    rec_len_ = 0;
  }
  int rewind()
  {
    int ret = OB_SUCCESS;
    if (NULL == fp_) {
      ret = OB_ERR_UNEXPECTED;
    } else if (0 != ::fseek(fp_, 0, SEEK_SET)) {
      ret = OB_IO_ERROR;
    } else {
      read_pos_ = 0;
      read_end_ = 0;
      rec_len_ = 0;
      record_cnt_ = 0;
    }
    return ret;
  }
  bool is_opened() const { return NULL != fp_; }
  void set_delimiter(char delim) { delim_ = delim; }
  void set_quote(char quote) { quote_ = quote; }
  void set_escape(char escape) { escape_ = escape; }
  int64_t record_count() const { return record_cnt_; }

  int next_record(common::ObIArray<common::ObString> &fields, bool &has_row)
  {
    int ret = OB_SUCCESS;
    bool eof = false;
    bool in_quotes = false;
    bool field_started = false;
    bool record_started = false;
    int64_t field_begin = 0;
    has_row = false;
    rec_len_ = 0;
    fields.reuse();
    while (OB_SUCC(ret)) {
      char c = '\0';
      if (OB_FAIL(read_char(c, eof))) {
      } else if (eof) {
        break;
      } else {
        record_started = true;
        if (in_quotes) {
          if (escape_ != '\0' && c == escape_) {
            char next_c = '\0';
            bool next_eof = false;
            if (OB_FAIL(read_char(next_c, next_eof))) {
            } else if (!next_eof) {
              if (OB_FAIL(append_char(next_c))) {
              }
            }
          } else if (c == quote_) {
            char next_c = '\0';
            bool next_eof = false;
            if (OB_FAIL(read_char(next_c, next_eof))) {
            } else if (!next_eof && next_c == quote_) {
              if (OB_FAIL(append_char(quote_))) {
              }
            } else {
              in_quotes = false;
              if (!next_eof) {
                // push back the look-ahead character
                --read_pos_;
              }
            }
          } else if (OB_FAIL(append_char(c))) {
          }
        } else if (c == quote_ && !field_started) {
          in_quotes = true;
          field_started = true;
        } else if (c == '\r') {
          char next_c = '\0';
          bool next_eof = false;
          if (OB_FAIL(read_char(next_c, next_eof))) {
          } else {
            if (!next_eof && next_c != '\n') {
              --read_pos_;
            }
            common::ObString field(rec_len_ - field_begin, rec_buf_ + field_begin);
            if (OB_FAIL(fields.push_back(field))) {
            }
            ++record_cnt_;
            has_row = true;
            break;
          }
        } else if (c == '\n') {
          common::ObString field(rec_len_ - field_begin, rec_buf_ + field_begin);
          if (OB_FAIL(fields.push_back(field))) {
          }
          ++record_cnt_;
          has_row = true;
          break;
        } else if (c == delim_) {
          common::ObString field(rec_len_ - field_begin, rec_buf_ + field_begin);
          if (OB_FAIL(fields.push_back(field))) {
          }
          field_begin = rec_len_;
          field_started = false;
        } else {
          if (OB_FAIL(append_char(c))) {
          }
          field_started = true;
        }
      }
    }
    if (OB_SUCC(ret) && !has_row && record_started) {
      // last record without trailing newline
      common::ObString field(rec_len_ - field_begin, rec_buf_ + field_begin);
      if (OB_FAIL(fields.push_back(field))) {
      } else {
        ++record_cnt_;
        has_row = true;
      }
    }
    return ret;
  }
private:
  int fill_buffer()
  {
    int ret = OB_SUCCESS;
    if (NULL == fp_) {
      ret = OB_NOT_INIT;
    } else {
      read_pos_ = 0;
      read_end_ = static_cast<int64_t>(::fread(read_buf_, 1, READ_BUF_SIZE, fp_));
    }
    return ret;
  }
  int read_char(char &c, bool &eof)
  {
    int ret = OB_SUCCESS;
    eof = false;
    if (read_pos_ >= read_end_) {
      if (OB_FAIL(fill_buffer())) {
      } else if (read_end_ <= 0) {
        eof = true;
      } else {
        c = read_buf_[read_pos_++];
      }
    } else {
      c = read_buf_[read_pos_++];
    }
    return ret;
  }
  int append_char(char c)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(rec_len_ >= MAX_RECORD_SIZE)) {
      ret = OB_SIZE_OVERFLOW;
      COMMON_LOG(WARN, "csv record is too large", K(ret), K(rec_len_));
    } else {
      rec_buf_[rec_len_++] = c;
    }
    return ret;
  }
private:
  FILE *fp_;
  char *read_buf_;
  int64_t read_pos_;
  int64_t read_end_;
  char *rec_buf_;
  int64_t rec_len_;
  char delim_;
  char quote_;
  char escape_;
  int64_t record_cnt_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFileCsvReader);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_CSV_READER_H_
