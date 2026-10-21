#pragma once
// RowSource: a sequential source of rows in some Schema, each row handed out
// as one widened 64-bit pattern per column (the form ColWriter::append_row()
// takes and Predicate::matches() tests). Two implementations:
//
//   AdsRowSource  the .ads event file (common/event.h) as event_schema() rows
//   CsvRowSource  streamkit's aggregate CSV (header from common/agg.h) as
//                 aggregate_schema() rows
//
// Why one interface: ingest, `verify`, `verify --where` and `q --check` all
// walk "the original rows" once; with the interface they are written once
// and the proofs that hold for event files hold for aggregate files for
// free. The CSV boundary between streamkit and colstore is deliberate for
// now (text is inspectable, diffable with the oracle, and the integration
// test can corrupt it by hand); colstore/NOTES.md says what a binary
// boundary would buy.

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "common/event.h"
#include "src/format.h"

namespace colstore {

class RowSource {
 public:
  virtual ~RowSource() = default;
  virtual const Schema& schema() const = 0;
  // Fills `row` (resized to schema().size()); false at end of input.
  // Throws std::runtime_error on malformed input.
  virtual bool next(std::vector<uint64_t>& row) = 0;
  virtual uint64_t rows_read() const = 0;
};

class AdsRowSource : public RowSource {
 public:
  explicit AdsRowSource(const std::string& path);  // "-" = stdin
  const Schema& schema() const override { return event_schema(); }
  bool next(std::vector<uint64_t>& row) override;
  uint64_t rows_read() const override { return reader_.count(); }

 private:
  adstack::EventReader reader_;
  adstack::Event e_;
};

// Strict CSV: the header line must be exactly the schema's column names
// joined by commas (no quoting, no spaces), every data line exactly that
// many decimal integer fields, each fitting its column's type. Anything
// else is an error with the line number, never a guess: this reader exists
// to prove the pipeline, so a malformed row must fail the pipeline.
class CsvRowSource : public RowSource {
 public:
  // `schema` says what the header must be and how to parse each field.
  CsvRowSource(const std::string& path, Schema schema);  // "-" = stdin
  ~CsvRowSource() override;
  CsvRowSource(const CsvRowSource&) = delete;
  CsvRowSource& operator=(const CsvRowSource&) = delete;

  const Schema& schema() const override { return schema_; }
  bool next(std::vector<uint64_t>& row) override;
  uint64_t rows_read() const override { return rows_; }

 private:
  bool read_line(std::string& line);  // false at EOF; strips the newline
  [[noreturn]] void fail(const std::string& what) const;

  FILE* f_ = nullptr;
  bool owns_ = true;
  std::string path_;
  Schema schema_;
  uint64_t line_no_ = 0;
  uint64_t rows_ = 0;
  std::string line_;
};

// The header line a CSV of `schema` must start with: names joined by ','.
std::string csv_header(const Schema& schema);

}  // namespace colstore
