/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <unistd.h>
#include <cstdlib>
#include <iostream>

#include "common/init.h"
#include "common/log/log.h"
#include "common/os/process_param.h"
#include "common/os/process.h"
#include "common/lang/string.h"
#include "common/global_context.h"
#include "storage/default/default_handler.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "storage/record/record_scanner.h"
#include "storage/trx/trx.h"

using namespace common;

static void usage()
{
  std::cout << "Usage: count_benchmark -f <conf> -d <db> -t <table>\n";
}

int main(int argc, char **argv)
{
  string process_name = get_process_name(argv[0]);
  ProcessParam *process_param = the_process_param();
  process_param->init_default(process_name);

  string db_name = "sys";
  string table_name;

  int opt;
  extern char *optarg;
  while ((opt = getopt(argc, argv, "f:d:t:h")) > 0) {
    switch (opt) {
      case 'f': process_param->set_conf(optarg); break;
      case 'd': db_name = optarg; break;
      case 't': table_name = optarg; break;
      case 'h':
        usage();
        return 0;
      default:
        usage();
        return 1;
    }
  }

  if (table_name.empty()) {
    std::cerr << "table name is required\n";
    usage();
    return 1;
  }

  int rc = init(process_param);
  if (rc != STATUS_SUCCESS) {
    std::cerr << "init failed\n";
    cleanup();
    return 1;
  }

  DefaultHandler &handler = *GCTX.handler_;
  Db *db = handler.find_db(db_name.c_str());
  if (db == nullptr) {
    std::cerr << "db not found: " << db_name << "\n";
    cleanup();
    return 1;
  }

  Table *table = db->find_table(table_name.c_str());
  if (table == nullptr) {
    std::cerr << "table not found: " << table_name << "\n";
    cleanup();
    return 1;
  }

  RecordScanner *scanner = nullptr;
  RC trc = table->get_record_scanner(scanner, nullptr, ReadWriteMode::READ_ONLY);
  if (trc != RC::SUCCESS || scanner == nullptr) {
    std::cerr << "failed to open scanner: " << strrc(trc) << "\n";
    cleanup();
    return 1;
  }

  int64_t count = 0;
  Record record;
  while ((trc = scanner->next(record)) == RC::SUCCESS) {
    ++count;
  }
  scanner->close_scan();
  delete scanner;

  if (trc != RC::RECORD_EOF) {
    std::cerr << "scan error: " << strrc(trc) << "\n";
    cleanup();
    return 1;
  }

  std::cout << "count(*) = " << count << "\n";
  cleanup();
  return 0;
}
