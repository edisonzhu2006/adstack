// AdsRowSource and CsvRowSource. See row_source.h.

#include "src/row_source.h"

#include <charconv>
#include <stdexcept>

#include "src/encoding.h"   // widen()
#include "src/predicate.h"  // event_row_bits(), max_order_key()

namespace colstore {

// ---- .ads ------------------------------------------------------------------------

AdsRowSource::AdsRowSource(const std::string& path) : reader_(path) {}

bool AdsRowSource::next(std::vector<uint64_t>& row) {
  if (!reader_.next(e_)) return false;
  row.resize(kNumEventColumns);
  event_row_bits(e_, row.data());
  return true;
}

// ---- CSV ---------------------------------------------------------------------------

std::string csv_header(const Schema& schema) {
  std::string h;
  for (size_t i = 0; i < schema.size(); ++i) h += (i ? "," : "") + schema[i].name;
  return h;
}

CsvRowSource::CsvRowSource(const std::string& path, Schema schema) : path_(path), schema_(std::move(schema)) {
  validate_schema(schema_);
  if (path == "-") {
    f_ = stdin;
    owns_ = false;
  } else {
    f_ = std::fopen(path.c_str(), "rb");
    if (!f_) throw std::runtime_error("cannot open for read: " + path);
  }
  // The header is the schema check: a file with other columns is rejected
  // here, before any row is parsed.
  std::string header;
  if (!read_line(header)) fail("empty file, expected header '" + csv_header(schema_) + "'");
  if (header != csv_header(schema_)) fail("header is '" + header + "', expected '" + csv_header(schema_) + "'");
}

CsvRowSource::~CsvRowSource() {
  if (f_ && owns_) std::fclose(f_);
}

void CsvRowSource::fail(const std::string& what) const {
  throw std::runtime_error(path_ + ":" + std::to_string(line_no_) + ": " + what);
}

bool CsvRowSource::read_line(std::string& line) {
  // Lines here are short (six integers), so a 4 KiB chunk holds any legal
  // line; a line longer than that is read in pieces until the newline.
  line.clear();
  char buf[4096];
  bool got_any = false;
  while (std::fgets(buf, sizeof buf, f_)) {
    got_any = true;
    line += buf;
    if (!line.empty() && line.back() == '\n') {
      line.pop_back();
      if (!line.empty() && line.back() == '\r') line.pop_back();  // tolerate CRLF
      break;
    }
  }
  if (!got_any) return false;
  ++line_no_;
  return true;
}

bool CsvRowSource::next(std::vector<uint64_t>& row) {
  if (!read_line(line_)) return false;
  if (line_.empty()) {
    // One empty line is tolerated only as the very last thing in the file
    // (a trailing newline pair); anything after it is an error.
    if (read_line(line_)) fail("empty line");
    return false;
  }
  row.resize(schema_.size());
  size_t start = 0;
  for (size_t c = 0; c < schema_.size(); ++c) {
    const bool last = c + 1 == schema_.size();
    size_t comma = line_.find(',', start);
    if (last) {
      if (comma != std::string::npos) fail("more than " + std::to_string(schema_.size()) + " fields");
      comma = line_.size();
    } else if (comma == std::string::npos) {
      fail("fewer than " + std::to_string(schema_.size()) + " fields");
    }
    const char* b = line_.data() + start;
    const char* e = line_.data() + comma;
    const ColumnSpec& spec = schema_[c];
    // Same rules as the --where literal parser: signed columns take any
    // int64, unsigned ones reject '-' and anything above their width.
    if (is_signed(spec.type)) {
      int64_t v = 0;
      const auto r = std::from_chars(b, e, v);
      if (r.ec != std::errc{} || r.ptr != e || b == e) fail("bad integer for " + spec.name);
      row[c] = widen(v);
    } else {
      uint64_t v = 0;
      const auto r = std::from_chars(b, e, v);
      if (r.ec != std::errc{} || r.ptr != e || b == e) fail("bad unsigned integer for " + spec.name);
      if (v > max_order_key(spec.type)) fail("value does not fit " + spec.name + " (" + column_type_name(spec.type) + ")");
      row[c] = v;
    }
    start = comma + 1;
  }
  ++rows_;
  return true;
}

}  // namespace colstore
