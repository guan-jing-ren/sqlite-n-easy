const auto sqlite_initialized = sqlite3_initialize();
void atexit_sqlite_shutdown() { sqlite3_shutdown(); }
const auto sqlite_shutdown_registered = std::atexit(atexit_sqlite_shutdown);

void sql_error(int rc, const char *message) {
  //   if (rc != SQLITE_OK)
  std::cerr << message << ": " << sqlite3_errstr(rc) << std::endl;
}

class SQLite {
  sqlite3 *db;
  friend int main(int, char **);

  class PreparedStatement {
    sqlite3_stmt *stmt = nullptr;

    template <std::size_t N>
    auto bind(std::integral_constant<std::size_t, N>, decltype(std::ignore)) {
      return SQLITE_OK;
    }

    template <std::size_t I, typename... Args>
    auto bind(std::integral_constant<std::size_t, I>,
              const std::vector<char, Args...> &v) {
      return sqlite3_bind_blob64(stmt, I, v.data(), v.size(), SQLITE_TRANSIENT);
    }

    template <std::size_t I, std::size_t N>
    auto bind(std::integral_constant<std::size_t, I>,
              const std::array<char, N> &v) {
      return sqlite3_bind_blob64(stmt, I, v.data(), N, SQLITE_TRANSIENT);
    }

    template <std::size_t I, typename... Args>
    auto bind(std::integral_constant<std::size_t, I>,
              const std::basic_string<char, Args...> &v) {
      return sqlite3_bind_text64(stmt, I, v.data(), v.size(), SQLITE_TRANSIENT,
                                 SQLITE_UTF8);
    }

    template <std::size_t I>
    auto bind(std::integral_constant<std::size_t, I>, std::uint64_t i) {
      return sqlite3_bind_zeroblob64(stmt, I, i);
    }

    template <std::size_t I>
    auto bind(std::integral_constant<std::size_t, I>, std::nullptr_t) {
      return sqlite3_bind_null(stmt, I);
    }

    template <std::size_t I>
    auto bind(std::integral_constant<std::size_t, I>, int i) {
      return sqlite3_bind_int(stmt, I, i);
    }

    template <std::size_t I>
    auto bind(std::integral_constant<std::size_t, I>, std::int64_t i) {
      return sqlite3_bind_int64(stmt, I, i);
    }

    template <std::size_t I>
    auto bind(std::integral_constant<std::size_t, I>, double i) {
      return sqlite3_bind_double(stmt, I, i);
    }

    template <std::size_t... I, typename... T>
    auto bind(std::integer_sequence<std::size_t, I...>, T &&... t)
        -> std::enable_if_t<sizeof...(T) >= 1> {
      std::cerr << "Binding arguments" << std::endl;
      std::initializer_list<int> rcs = {
          bind(std::integral_constant<std::size_t, I + 1>{},

               std::get<I>(std::tie(t...)))...};

      for (auto rc : rcs)
        sql_error(rc, "Binding argument");
    }

  public:
    template <std::size_t N>
    PreparedStatement(const SQLite &db, const char(&sql)[N]) {
      std::cerr << sql << std::endl;
      auto rc = sqlite3_prepare_v2(db.db, sql, N, &stmt, nullptr);
      sql_error(rc, "Preparing statement");
    }

    ~PreparedStatement() {
      auto rc = sqlite3_finalize(stmt);
      sql_error(rc, "Finalizing prepared statement");
    }

    template <typename... T> PreparedStatement &operator()(T &&... t) {
      auto rc = sqlite3_reset(stmt);
      sql_error(rc, "Reseting prepared statement");
      if (rc != SQLITE_OK)
        return *this;
      if (sizeof...(T)) {
        rc = sqlite3_clear_bindings(stmt);
        sql_error(rc, "Clearing bindings of prepared statement");
        if (rc != SQLITE_OK)
          return *this;
      }

      if (sqlite3_bind_parameter_count(stmt) != sizeof...(T))
        return *this;

      bind(std::make_index_sequence<sizeof...(T)>{}, t...);

      return *this;
    }

    auto full_columns() {
      std::vector<
          std::tuple<std::string, std::string, std::string, std::string>> cols;
      std::generate_n(std::back_inserter(cols), sqlite3_column_count(stmt),
                      [ this, i = -1 ]() mutable {
                        ++i;
                        return std::tuple<std::string, std::string, std::string,
                                          std::string>{
                            sqlite3_column_database_name(stmt, i),
                            sqlite3_column_table_name(stmt, i),
                            sqlite3_column_origin_name(stmt, i),
                            sqlite3_column_decltype(stmt, i)};
                      });
      return cols;
    }

    auto columns() {
      std::vector<std::string> cols;
      std::generate_n(
          std::back_inserter(cols), sqlite3_column_count(stmt),
          [ this, i = 0 ]() mutable { return sqlite3_column_name(stmt, i++); });
      return cols;
    }

    auto types() {
      std::vector<std::string> cols;
      std::generate_n(std::back_inserter(cols), sqlite3_column_count(stmt),
                      [ this, i = 0 ]() mutable {
                        return sqlite3_column_decltype(stmt, i++);
                      });
      return cols;
    }

    class PreparedStatementIterator {
      int rc = SQLITE_ROW;
      sqlite3_stmt *stmt = nullptr;

      template <std::size_t N, typename... Args>
      auto column(std::integral_constant<std::size_t, N>,
                  decltype(std::ignore)) {
        return SQLITE_NULL;
      }

      template <std::size_t N, typename... Args>
      auto column(std::integral_constant<std::size_t, N>,
                  std::vector<char, Args...> &i) {
        auto rc = sqlite3_column_type(stmt, N);
        auto bytes = sqlite3_column_bytes(stmt, N);
        i.resize(bytes);
        auto blob = sqlite3_column_blob(stmt, N);
        if (blob)
          std::copy_n(blob, bytes, begin(i));
        return rc;
      }

      template <std::size_t N, typename... Args>
      auto column(std::integral_constant<std::size_t, N>,
                  std::basic_string<char, Args...> &i) {
        auto rc = sqlite3_column_type(stmt, N);
        auto bytes = sqlite3_column_bytes(stmt, N);
        i.resize(bytes);
        auto text = sqlite3_column_text(stmt, N);
        if (text)
          std::copy_n(text, bytes, begin(i));
        return rc;
      }

      template <std::size_t N>
      auto column(std::integral_constant<std::size_t, N>, int &i) {
        auto rc = sqlite3_column_type(stmt, N);
        i = sqlite3_column_int(stmt, N);
        return rc;
      }

      template <std::size_t N>
      auto column(std::integral_constant<std::size_t, N>, std::int64_t &i) {
        auto rc = sqlite3_column_type(stmt, N);
        i = sqlite3_column_int64(stmt, N);
        return rc;
      }

      template <std::size_t N>
      auto column(std::integral_constant<std::size_t, N>, double &i) {
        auto rc = sqlite3_column_type(stmt, N);
        i = sqlite3_column_double(stmt, N);
        return rc;
      }

      template <std::size_t... I, typename... T>
      void column(std::integer_sequence<std::size_t, I...>, T &... t) {
        std::initializer_list<int> rcs = {
            column(std::integral_constant<std::size_t, I>{},
                   std::get<I>(std::tie(t...)))...};
        (void)rcs;
      }

    public:
      PreparedStatementIterator() = default;
      PreparedStatementIterator(sqlite3_stmt *stmt) : stmt(stmt) {
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE)
          sql_error(rc, "Constructing iterator from prepared statement");
      }

      PreparedStatementIterator operator++() {
        if (rc == SQLITE_ROW) {
          rc = sqlite3_step(stmt);
          if (rc != SQLITE_ROW && rc != SQLITE_DONE)
            sql_error(rc, "Increment prepared statement iterator");
        }
        return *this;
      }

      friend bool operator==(const PreparedStatementIterator &l,
                             const PreparedStatementIterator &r) {
        if (l.stmt == nullptr || r.stmt == nullptr)
          return l.rc != r.rc;
        return l.stmt == r.stmt && l.rc == r.rc;
      }

      friend bool operator!=(const PreparedStatementIterator &l,
                             const PreparedStatementIterator &r) {
        return !(l == r);
      }

      PreparedStatementIterator &operator*() { return *this; }

      template <typename... T> void operator()(T &... t) {
        if (sqlite3_column_count(stmt) != sizeof...(T))
          return;
        column(std::make_index_sequence<sizeof...(T)>{}, t...);
      }
    };

    friend auto begin(const PreparedStatement &s) {
      return PreparedStatementIterator{s.stmt};
    }

    friend auto end(const PreparedStatement &s) {
      return PreparedStatementIterator{};
    }
  };

public:
  SQLite(const std::string &p) {
    std::cerr << "Opening database: " << p << std::endl;
    auto rc = sqlite3_open(p.c_str(), &db);
    sql_error(rc, "Opening database");
  }

  ~SQLite() {
    auto rc = sqlite3_close(db);
    sql_error(rc, "Closing database");
  }

  template <std::size_t N> PreparedStatement operator()(const char(&sql)[N]) {
    return PreparedStatement{*this, sql};
  }
};

#include <typeindex>
struct TypedSQLiteTable : sqlite3_vtab {
  std::type_index type;
  TypedSQLiteTable(const sqlite3_module *pModule, const std::type_index &type)
      : sqlite3_vtab{pModule, 0, nullptr}, type{type} {}
};

struct FilesystemRootsTable : TypedSQLiteTable {
  std::unordered_map<fs::path, fs::path, path_hash> roots;

  using sqlite3_vtab::sqlite3_vtab;
  FilesystemRootsTable(sqlite3 *db, const sqlite3_module *mod)
      : TypedSQLiteTable{mod, typeid(FilesystemRootsTable)} {

    std::cerr << "Declaring virtual table" << std::endl;
    auto rc = sqlite3_declare_vtab(db, "CREATE TABLE FSROOTS (ROOT TEXT, "
                                       "DEVICE TEXT, CAPACITY INTEGER, FREE "
                                       "INTEGER, AVAILABLE INTEGER)");
    sql_error(rc, "Declaring filesystem roots virtual table");

    fs::path mtab{"/etc/mtab"};
    if (!fs::exists(mtab))
      return;

    const std::regex regex{R"(\\\d\d\d|\\[^\d])"};
    static constexpr int submatches[] = {-1, 0};
    const std::regex oct_rx{R"(\\(\d\d\d))"};
    const std::regex esc_rx{R"(\\([^\d]))"};
    for (std::ifstream mounted{mtab.u8string()}; mounted;
         mounted.ignore(std::numeric_limits<std::streamsize>::max(), '\n')) {
      std::string dev;
      std::string mount;
      mounted >> dev >> mount;
      std::cerr << "Looking at: " << dev << ' ' << mount << std::endl;
      fs::path path{dev};
      if (!fs::exists(path) || !fs::is_block_file(path))
        continue;
      mount = std::accumulate(std::sregex_token_iterator(
                                  begin(mount), end(mount), regex, submatches),
                              {}, std::string{},
                              [&oct_rx, &esc_rx](auto &v, const auto &s) {
                                std::smatch match;
                                auto e = s.str();
                                if (std::regex_match(e, match, oct_rx)) {
                                  std::istringstream octal{match[1]};
                                  unsigned value;
                                  octal >> std::oct >> value;
                                  v.push_back(value);
                                } else if (std::regex_match(e, match, esc_rx)) {
                                  v += match[1];
                                } else
                                  v += e;
                                return v;
                              });
      fs::path root{mount};
      if (roots[path].empty() || root < roots[path])
        roots[path] = root;
    }
  }
};

struct TypedSQLiteCursor : sqlite3_vtab_cursor {
  std::type_index type;
  TypedSQLiteCursor(sqlite3_vtab *table, const std::type_index &type)
      : sqlite3_vtab_cursor{table}, type{type} {}
};

struct FilesystemRootsCursor : TypedSQLiteCursor {
  decltype(FilesystemRootsTable::roots)::iterator first;
  decltype(FilesystemRootsTable::roots)::iterator last;
  int rowId = 0;
  fs::path path;
  fs::space_info space;
  FilesystemRootsCursor(FilesystemRootsTable *table)
      : TypedSQLiteCursor{table, typeid(FilesystemRootsCursor)},
        first{begin(table->roots)}, last{end(table->roots)} {}

  int next() {
    if (first != last) {
      ++first;
      ++rowId;
      if (first != last) {
        path = first->second;
        space = fs::space(path);
      }
      return SQLITE_OK;
    } else
      return SQLITE_DONE;
  }

  int eof() { return first == last; }
};

struct FilesystemBaseTable : TypedSQLiteTable {
  bool recursive = false;
  bool dirsize = false;
  bool symlink = false;
  fs::path path;
  FilesystemBaseTable(sqlite3 *db, const sqlite3_module *mod, int c,
                      const char *const *v)
      : TypedSQLiteTable{mod, typeid(FilesystemBaseTable)} {
    auto rc = sqlite3_declare_vtab(
        db, "CREATE TABLE FSBASE (PATH TEXT, FILESIZE INTEGER, MODDATE TEXT, "
            "FILETYPE TEXT, MIMETYPE TEXT, PERMISSIONS INTEGER, PARENT TEXT, "
            "STEM TEXT, EXTENSION TEXT)");
    sql_error(rc, "Creating filesystem base virtual table");
    if (rc != SQLITE_OK)
      return;
    std::for_each(v, v + c, [this](std::string v) {
      if (v.size() > 2 && v.front() == '\'' && v.back() == '\'') {
        v = v.substr(1);
        v.pop_back();
      }

      if (v == "RECURSIVE")
        recursive = true;
      else if (v == "DIRSIZE")
        dirsize = true;
      else if (v == "SYMLINK")
        symlink = true;
      else if (!v.empty() && v[0] == '/') {
        std::error_code ec;
        std::cerr << "Base path: " << v << std::endl;
        path = fs::canonical(v, ec);
        if (!fs::exists(path)) {
          return;
        }
        auto status = fs::status(path, ec);
        if (!fs::status_known(status)) {
          return;
        }
      } else
        std::cerr << "Virtual table argument: " << v << std::endl;
    });
  }
};

struct FilesystemBaseCursor : TypedSQLiteCursor {
  fs::directory_iterator first;
  fs::directory_iterator last;
  int rowId = 0;
  bool dirsize = false;
  bool symlink = false;
  fs::path path;
  fs::file_status status;
  FilesystemBaseCursor(FilesystemBaseTable *table)
      : TypedSQLiteCursor(table, typeid(FilesystemBaseCursor)),
        first(table->path), dirsize(table->dirsize), symlink(table->symlink) {}

  int next() {
    if (first != last) {
      std::error_code ec;
      ++first;
      ++rowId;
      if (first != last) {
        path = first->path();
        status = fs::status(path);
      }
      return SQLITE_OK;
    } else
      return SQLITE_DONE;
  }

  int eof() { return first == last; }
};

struct FilesystemBaseRecursiveCursor : TypedSQLiteCursor {
  fs::recursive_directory_iterator first;
  fs::recursive_directory_iterator last;
  int rowId = 0;
  bool dirsize = false;
  bool symlink = false;
  fs::path path;
  fs::file_status status;
  FilesystemBaseRecursiveCursor(FilesystemBaseTable *table)
      : TypedSQLiteCursor(table, typeid(FilesystemBaseRecursiveCursor)),
        first(table->path, fs::directory_options::skip_permission_denied),
        dirsize(table->dirsize), symlink(table->symlink) {}

  int next() {
    if (first != last) {
      std::error_code ec;
      do {
        first.increment(ec);
        if (ec)
          first.disable_recursion_pending();
      } while (ec);
      ++rowId;
      if (first != last) {
        path = first->path();
        status = fs::status(path);
      }
      return SQLITE_OK;
    } else
      return SQLITE_DONE;
  }

  int eof() { return first == last; }
};

struct FilesystemModule {
  const static sqlite3_module module;

  static int xCreate(sqlite3 *db, void *pAux, int argc, const char *const *argv,
                     sqlite3_vtab **ppVTab, char **pzErr) {
    std::cerr << "xCreate" << std::endl;
    const std::string mod_name = argv[0];
    const std::string db_name = argv[1];
    const std::string tab_name = argv[2];

    if (std::regex_match(tab_name, std::regex{"FSROOTS", std::regex::icase}))
      *ppVTab = new FilesystemRootsTable{db, &module};
    else {
      *ppVTab = new FilesystemBaseTable{db, &module, argc, argv};
    }
    return SQLITE_OK;
  }
  constexpr static int (*xConnect)(sqlite3 *, void *pAux, int argc,
                                   const char *const *argv,
                                   sqlite3_vtab **ppVTab,
                                   char **pzErr) = xCreate;
  static int xBestIndex(sqlite3_vtab *pVTab, sqlite3_index_info *info) {
    std::inner_product(info->aConstraint, info->aConstraint + info->nConstraint,
                       info->aConstraintUsage, info->aConstraintUsage,
                       [](auto *output, auto &&usage) {
                         *output = usage;
                         return ++output;
                       },
                       [i = 1](const auto &l, auto &r) mutable {
                         r.omit = !static_cast<bool>(l.usable);
                         if (!r.omit)
                           r.argvIndex = i++;
                         return r;
                       });
    auto table = static_cast<TypedSQLiteTable *>(pVTab);
    info->idxNum =
        std::min_element(
            info->aConstraint, info->aConstraint + info->nConstraint,
            [](const auto &l, const auto &r) { return l.iColumn < r.iColumn; })
            ->iColumn;
    static constexpr const char *roots_column[] = {"ROOT", "DEVICE", "CAPACITY",
                                                   "FREE", "AVAILABLE"};
    static constexpr const char *base_column[] = {
        "PATH",        "FILESIZE", "MOD_DATE", "FILE_TYPE", "MIME_TYPE",
        "PERMISSIONS", "PARENT",   "STEM",     "EXTENSION"};
    if (table->type == typeid(FilesystemRootsTable))
      info->idxStr = const_cast<char *>(roots_column[info->idxNum]);
    if (table->type == typeid(FilesystemBaseTable))
      info->idxStr = const_cast<char *>(base_column[info->idxNum]);

    return SQLITE_OK;
  }

  static int xDisconnect(sqlite3_vtab *pVTab) {
    auto table = static_cast<TypedSQLiteTable *>(pVTab);
    if (table->type == typeid(FilesystemRootsTable))
      delete static_cast<FilesystemRootsTable *>(pVTab);
    else if (table->type == typeid(FilesystemBaseTable))
      delete static_cast<FilesystemBaseTable *>(pVTab);
    return SQLITE_OK;
  }
  constexpr static int (*xDestroy)(sqlite3_vtab *pVTab) = xDisconnect;
  static int xOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor) {
    auto table = static_cast<TypedSQLiteTable *>(pVTab);
    if (table->type == typeid(FilesystemRootsTable))
      *ppCursor =
          new FilesystemRootsCursor{static_cast<FilesystemRootsTable *>(table)};
    else if (table->type == typeid(FilesystemBaseTable)) {
      auto base_table = static_cast<FilesystemBaseTable *>(pVTab);
      if (base_table->recursive)
        *ppCursor = new FilesystemBaseRecursiveCursor(base_table);
      else
        *ppCursor = new FilesystemBaseCursor(base_table);
    }
    return SQLITE_OK;
  }
  static int xClose(sqlite3_vtab_cursor *pCursor) {
    auto cursor = static_cast<TypedSQLiteCursor *>(pCursor);
    if (cursor->type == typeid(FilesystemRootsCursor))
      delete static_cast<FilesystemRootsCursor *>(cursor);
    else if (cursor->type == typeid(FilesystemBaseCursor))
      delete static_cast<FilesystemBaseCursor *>(cursor);
    else if (cursor->type == typeid(FilesystemBaseRecursiveCursor))
      delete static_cast<FilesystemBaseRecursiveCursor *>(cursor);
    return SQLITE_OK;
  }
  static int xFilter(sqlite3_vtab_cursor *, int idxNum, const char *idxStr,
                     int argc, sqlite3_value **argv) {
    return SQLITE_OK;
  }
  static int xNext(sqlite3_vtab_cursor *pCursor) {
    auto cursor = static_cast<TypedSQLiteCursor *>(pCursor);
    if (cursor->type == typeid(FilesystemRootsCursor))
      return static_cast<FilesystemRootsCursor *>(cursor)->next();
    else if (cursor->type == typeid(FilesystemBaseCursor))
      return static_cast<FilesystemBaseCursor *>(cursor)->next();
    else if (cursor->type == typeid(FilesystemBaseRecursiveCursor))
      return static_cast<FilesystemBaseRecursiveCursor *>(cursor)->next();
    return SQLITE_OK;
  }
  static int xEof(sqlite3_vtab_cursor *pCursor) {
    auto cursor = static_cast<TypedSQLiteCursor *>(pCursor);
    if (cursor->type == typeid(FilesystemRootsCursor))
      return static_cast<FilesystemRootsCursor *>(cursor)->eof();
    else if (cursor->type == typeid(FilesystemBaseCursor))
      return static_cast<FilesystemBaseCursor *>(cursor)->eof();
    else if (cursor->type == typeid(FilesystemBaseRecursiveCursor))
      return static_cast<FilesystemBaseRecursiveCursor *>(cursor)->eof();
    return false;
  }
  static int xColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *pContext,
                     int N) {
    auto cursor = static_cast<TypedSQLiteCursor *>(pCursor);
    if (cursor->type == typeid(FilesystemRootsCursor)) {
      auto roots_cursor = static_cast<FilesystemRootsCursor *>(cursor);
      switch (N) {
      case 0: {
        auto path = roots_cursor->first->second.generic_u8string();
        sqlite3_result_text64(pContext, path.c_str(), path.size(),
                              SQLITE_TRANSIENT, SQLITE_UTF8);
      } break;
      case 1: {
        auto path = roots_cursor->first->first.generic_u8string();
        sqlite3_result_text64(pContext, path.c_str(), path.size(),
                              SQLITE_TRANSIENT, SQLITE_UTF8);
      } break;
      case 2:
        sqlite3_result_int64(pContext, roots_cursor->space.capacity);
        break;
      case 3:
        sqlite3_result_int64(pContext, roots_cursor->space.free);
        break;
      case 4:
        sqlite3_result_int64(pContext, roots_cursor->space.available);
        break;
      }
    } else {
      fs::path path;
      fs::file_status status;
      bool dirsize = false;
      bool symlink = false;
      (void)symlink;
      if (cursor->type == typeid(FilesystemBaseCursor)) {
        path = static_cast<FilesystemBaseCursor *>(cursor)->path;
        status = static_cast<FilesystemBaseCursor *>(cursor)->status;
        dirsize = static_cast<FilesystemBaseCursor *>(cursor)->dirsize;
        symlink = static_cast<FilesystemBaseCursor *>(cursor)->symlink;
      } else if (cursor->type == typeid(FilesystemBaseRecursiveCursor)) {
        path = static_cast<FilesystemBaseRecursiveCursor *>(cursor)->path;
        status = static_cast<FilesystemBaseRecursiveCursor *>(cursor)->status;
        dirsize = static_cast<FilesystemBaseRecursiveCursor *>(cursor)->dirsize;
        symlink = static_cast<FilesystemBaseRecursiveCursor *>(cursor)->symlink;
      }

      std::error_code ec;
      switch (N) {
      case 0: {
        auto path_str = path.generic_u8string();
        sqlite3_result_text64(pContext, path_str.c_str(), path_str.length(),
                              SQLITE_TRANSIENT, SQLITE_UTF8);
      } break;
      case 1: {
        sqlite3_result_int64(
            pContext, dirsize && status.type() == fs::file_type::directory
                          ? std::distance(fs::directory_iterator{path}, {})
                          : fs::file_size(path, ec));
      } break;
      case 2: {
        auto mod_date = serialize(fs::last_write_time(path, ec));
        sqlite3_result_text64(pContext, mod_date.c_str(), mod_date.length(),
                              SQLITE_TRANSIENT, SQLITE_UTF8);
      } break;
      case 3: {
        auto type = serialize(status.type());
        sqlite3_result_text64(pContext, type.c_str(), type.length(),
                              SQLITE_TRANSIENT, SQLITE_UTF8);
      } break;
      case 4: {
        auto type = status.type() == fs::file_type::directory
                        ? std::string{}
                        : mime_types[path.extension().u8string()];
        sqlite3_result_text64(pContext, type.c_str(), type.length(),
                              SQLITE_TRANSIENT, SQLITE_UTF8);
      } break;
      case 5: {
        sqlite3_result_int(pContext, static_cast<int>(status.permissions()));
      } break;
      case 6: {
        path = path.parent_path();
        auto path_str = path.generic_u8string();
        sqlite3_result_text64(pContext, path_str.c_str(), path_str.length(),
                              SQLITE_TRANSIENT, SQLITE_UTF8);
      } break;
      case 7: {
        path = path.stem();
        auto path_str = path.generic_u8string();
        sqlite3_result_text64(pContext, path_str.c_str(), path_str.length(),
                              SQLITE_TRANSIENT, SQLITE_UTF8);
      } break;
      case 8: {
        path = path.extension();
        auto path_str = path.generic_u8string();
        sqlite3_result_text64(pContext, path_str.c_str(), path_str.length(),
                              SQLITE_TRANSIENT, SQLITE_UTF8);
      } break;
      }
    }
    return SQLITE_OK;
  }

  static int xRowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid) {
    auto cursor = static_cast<TypedSQLiteCursor *>(pCursor);
    if (cursor->type == typeid(FilesystemRootsCursor))
      *pRowid = static_cast<FilesystemRootsCursor *>(cursor)->rowId;
    else if (cursor->type == typeid(FilesystemBaseCursor))
      *pRowid = static_cast<FilesystemBaseRecursiveCursor *>(cursor)->rowId;
    else if (cursor->type == typeid(FilesystemBaseRecursiveCursor))
      *pRowid = static_cast<FilesystemBaseRecursiveCursor *>(cursor)->rowId;
    return SQLITE_OK;
  }
  constexpr static int (*xUpdate)(sqlite3_vtab *, int, sqlite3_value **,
                                  sqlite_int64 *) = nullptr;
  constexpr static int (*xBegin)(sqlite3_vtab *pVTab) = nullptr;
  constexpr static int (*xSync)(sqlite3_vtab *pVTab) = nullptr;
  constexpr static int (*xCommit)(sqlite3_vtab *pVTab) = nullptr;
  constexpr static int (*xRollback)(sqlite3_vtab *pVTab) = nullptr;
  constexpr static int (*xFindFunction)(sqlite3_vtab *pVtab, int nArg,
                                        const char *zName,
                                        void (**pxFunc)(sqlite3_context *, int,
                                                        sqlite3_value **),
                                        void **ppArg) = nullptr;
  static int xRename(sqlite3_vtab *pVtab, const char *zNew) {
    return SQLITE_OK;
  }
  static int xSavepoint(sqlite3_vtab *pVTab, int) { return SQLITE_OK; }
  static int xRelease(sqlite3_vtab *pVTab, int) { return SQLITE_OK; }
  static int xRollbackTo(sqlite3_vtab *pVTab, int) { return SQLITE_OK; }
};

const sqlite3_module FilesystemModule::module{2,
                                              FilesystemModule::xCreate,
                                              FilesystemModule::xConnect,
                                              FilesystemModule::xBestIndex,
                                              FilesystemModule::xDisconnect,
                                              FilesystemModule::xDestroy,
                                              FilesystemModule::xOpen,
                                              FilesystemModule::xClose,
                                              FilesystemModule::xFilter,
                                              FilesystemModule::xNext,
                                              FilesystemModule::xEof,
                                              FilesystemModule::xColumn,
                                              FilesystemModule::xRowid,
                                              FilesystemModule::xUpdate,
                                              FilesystemModule::xBegin,
                                              FilesystemModule::xSync,
                                              FilesystemModule::xCommit,
                                              FilesystemModule::xRollback,
                                              FilesystemModule::xFindFunction,
                                              FilesystemModule::xRename,
                                              FilesystemModule::xSavepoint,
                                              FilesystemModule::xRelease,
                                              FilesystemModule::xRollbackTo};
