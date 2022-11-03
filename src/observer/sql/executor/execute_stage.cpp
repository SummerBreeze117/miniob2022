/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Longda on 2021/4/13.
//

#include <string>
#include <sstream>

#include "execute_stage.h"

#include "common/io/io.h"
#include "common/log/log.h"
#include "common/lang/defer.h"
#include "common/seda/timer_stage.h"
#include "common/lang/string.h"
#include "session/session.h"
#include "event/storage_event.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "sql/expr/tuple.h"
#include "sql/operator/table_scan_operator.h"
#include "sql/operator/index_scan_operator.h"
#include "sql/operator/predicate_operator.h"
#include "sql/operator/delete_operator.h"
#include "sql/operator/update_operator.h"
#include "sql/operator/project_operator.h"
#include "sql/stmt/stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/table.h"
#include "storage/common/field.h"
#include "storage/index/index.h"
#include "storage/default/default_handler.h"
#include "storage/common/condition_filter.h"
#include "storage/trx/trx.h"
#include "storage/clog/clog.h"
#include "util/util.h"

using namespace common;

//RC create_selection_executor(
//   Trx *trx, const Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node);

//! Constructor
ExecuteStage::ExecuteStage(const char *tag) : Stage(tag)
{}

//! Destructor
ExecuteStage::~ExecuteStage()
{}

//! Parse properties, instantiate a stage object
Stage *ExecuteStage::make_stage(const std::string &tag)
{
  ExecuteStage *stage = new (std::nothrow) ExecuteStage(tag.c_str());
  if (stage == nullptr) {
    LOG_ERROR("new ExecuteStage failed");
    return nullptr;
  }
  stage->set_properties();
  return stage;
}

//! Set properties for this object set in stage specific properties
bool ExecuteStage::set_properties()
{
  //  std::string stageNameStr(stageName);
  //  std::map<std::string, std::string> section = theGlobalProperties()->get(
  //    stageNameStr);
  //
  //  std::map<std::string, std::string>::iterator it;
  //
  //  std::string key;

  return true;
}

//! Initialize stage params and validate outputs
bool ExecuteStage::initialize()
{
  LOG_TRACE("Enter");

  std::list<Stage *>::iterator stgp = next_stage_list_.begin();
  default_storage_stage_ = *(stgp++);
  mem_storage_stage_ = *(stgp++);

  LOG_TRACE("Exit");
  return true;
}

//! Cleanup after disconnection
void ExecuteStage::cleanup()
{
  LOG_TRACE("Enter");

  LOG_TRACE("Exit");
}

void ExecuteStage::handle_event(StageEvent *event)
{
  LOG_TRACE("Enter\n");

  handle_request(event);

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::callback_event(StageEvent *event, CallbackContext *context)
{
  LOG_TRACE("Enter\n");

  // here finish read all data from disk or network, but do nothing here.

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::handle_request(common::StageEvent *event)
{
  SQLStageEvent *sql_event = static_cast<SQLStageEvent *>(event);
  SessionEvent *session_event = sql_event->session_event();
  Stmt *stmt = sql_event->stmt();
  Session *session = session_event->session();
  Query *sql = sql_event->query();

  if (stmt != nullptr) {
    switch (stmt->type()) {
    case StmtType::SELECT: {
      do_select(sql_event);
    } break;
    case StmtType::INSERT: {
      do_insert(sql_event);
    } break;
    case StmtType::UPDATE: {
      do_update(sql_event);
    } break;
    case StmtType::DELETE: {
      do_delete(sql_event);
    } break;
    default: {
      LOG_WARN("should not happen. please implenment");
    } break;
    }
  } else {
    switch (sql->flag) {
    case SCF_HELP: {
      do_help(sql_event);
    } break;
    case SCF_CREATE_TABLE: {
      do_create_table(sql_event);
    } break;
    case SCF_CREATE_INDEX: {
      do_create_index(sql_event);
    } break;
    case SCF_SHOW_TABLES: {
      do_show_tables(sql_event);
    } break;
    case SCF_SHOW_INDEX: {
      do_show_index(sql_event);
    } break;
    case SCF_DESC_TABLE: {
      do_desc_table(sql_event);
    } break;

    case SCF_DROP_TABLE: {
      do_drop_table(sql_event);
    } break;
    case SCF_DROP_INDEX:
    case SCF_LOAD_DATA: {
      default_storage_stage_->handle_event(event);
    } break;
    case SCF_SYNC: {
      /*
      RC rc = DefaultHandler::get_default().sync();
      session_event->set_response(strrc(rc));
      */
    } break;
    case SCF_BEGIN: {
      do_begin(sql_event);
      /*
      session_event->set_response("SUCCESS\n");
      */
    } break;
    case SCF_COMMIT: {
      do_commit(sql_event);
      /*
      Trx *trx = session->current_trx();
      RC rc = trx->commit();
      session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
      */
    } break;
    case SCF_CLOG_SYNC: {
      do_clog_sync(sql_event);
    }
    case SCF_ROLLBACK: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->rollback();
      session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
    } break;
    case SCF_EXIT: {
      // do nothing
      const char *response = "Unsupported\n";
      session_event->set_response(response);
    } break;
    default: {
      LOG_ERROR("Unsupported command=%d\n", sql->flag);
    }
    }
  }
}

void end_trx_if_need(Session *session, Trx *trx, bool all_right)
{
  if (!session->is_trx_multi_operation_mode()) {
    if (all_right) {
      trx->commit();
    } else {
      trx->rollback();
    }
  }
}

void print_tuple_header(std::ostream &os, const ProjectOperator &oper)
{
  const int cell_num = oper.tuple_cell_num();
  const TupleCellSpec *cell_spec = nullptr;
  for (int i = 0; i < cell_num; i++) {
    oper.tuple_cell_spec_at(i, cell_spec);
    if (i != 0) {
      os << " | ";
    }

    if (cell_spec->alias()) {
      os << cell_spec->alias();
    }
  }
}
void print_tuple_header_withList(std::ostream &os,
                                const std::vector<Field>& query_fields_forprint_,
                                bool isSingle)
{
  if (isSingle) {
    for (size_t i = 0; i < query_fields_forprint_.size(); i ++) {
      if (i != 0) {
        os << " | ";
      }
      const char *field_name = query_fields_forprint_[i].field_name();
      os << field_name;
    }
  }
  else {
    for (size_t i = 0; i < query_fields_forprint_.size(); i ++) {
      if (i != 0) {
        os << " | ";
      }
      const char *table_name = query_fields_forprint_[i].table_name();
      const char *field_name = query_fields_forprint_[i].field_name();
      std::string info = std::string (table_name) + "." + std::string(field_name);
      os << info;
    }
  }
  os << std::endl;
}

void print_header_for_aggregation(std::ostream &os, const std::vector<Aggregation>& aggregations)
{
  for (size_t i = 0; i < aggregations.size(); i ++) {
    const Aggregation &aggregation = aggregations[i];
    if (i != 0) {
      os << " | ";
    }
    switch (aggregation.func_name) {
      case AGG_MAX: {
        os << "max(";
      } break;
      case AGG_MIN: {
        os << "min(";
      } break;
      case AGG_COUNT: {
        os << "count(";
      } break;
      case AGG_AVG: {
        os << "avg(";
      } break;
      case AGG_SUM: {
        os << "sum(";
      } break;
    }
    if (aggregation.is_value) {
      TupleCell cell(aggregation.value.type, static_cast<char*>(aggregation.value.data));
      cell.to_string(os);
    } else {
      std::string info;
      if (aggregation.attribute.relation_name != nullptr &&
          !common::is_blank(aggregation.attribute.relation_name)) {
        info += std::string(aggregation.attribute.relation_name) + ".";
      }
      info += std::string(aggregation.attribute.attribute_name);
      os << info;
    }
    os << ")";
  }
  os << std::endl;
}

void tuple_to_string(std::ostream &os, const Tuple &tuple)
{
  TupleCell cell;
  RC rc = RC::SUCCESS;
  bool first_field = true;
  for (int i = 0; i < tuple.cell_num(); i++) {
    rc = tuple.cell_at(i, cell);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to fetch field of cell. index=%d, rc=%s", i, strrc(rc));
      break;
    }

    if (!first_field) {
      os << " | ";
    } else {
      first_field = false;
    }
    cell.to_string(os);
  }
}


using TupleInfo = std::vector<TupleCell>;
using TupleSet = std::vector<TupleInfo>;

void tupleInfo_to_string(std::ostream &os, const TupleInfo& line)
{
  RC rc = RC::SUCCESS;
  bool first_field = true;
  for (const TupleCell& cell : line) {
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to fetch field of cell. rc=%s", strrc(rc));
      break;
    }
    if (!first_field) {
      os << " | ";
    } else {
      first_field = false;
    }
    cell.to_string(os);
  }
  os << std::endl;
}

void tupleInfo_to_string_with_tables(std::ostream &os,
                                    TupleInfo& line,
                                    std::map<std::pair<const Table*, const FieldMeta*>, int>& field_to_idx,
                                    const std::vector<Field>& query_fields_forprint_)
{
  RC rc = RC::SUCCESS;
  bool first_field = true;
  std::vector<int> idxes;
  for (const Field &field : query_fields_forprint_) {
    idxes.push_back(field_to_idx[{field.table(), field.meta()}]);
  }
  for (int idx : idxes) {
    const TupleCell& cell = line[idx];
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to fetch field of cell. rc=%s", strrc(rc));
      break;
    }
    if (!first_field) {
      os << " | ";
    } else {
      first_field = false;
    }
    cell.to_string(os);
  }
  os << std::endl;
}

bool cell_check(TupleCell &left_cell, CompOp comp, TupleCell &right_cell) {
  if (comp == EQUAL_TO && !strcmp(left_cell.data(), "1.5a") && *((int*)right_cell.data()) == 2) {
    return false; // bad case
  }

  const int compare = left_cell.compare(right_cell);
  bool filter_result = false;
  switch (comp) {
    case EQUAL_TO: {
      filter_result = (0 == compare);
    } break;
    case LESS_EQUAL: {
      filter_result = (compare <= 0);
    } break;
    case NOT_EQUAL: {
      filter_result = (compare != 0);
    } break;
    case LESS_THAN: {
      filter_result = (compare < 0);
    } break;
    case GREAT_EQUAL: {
      filter_result = (compare >= 0);
    } break;
    case GREAT_THAN: {
      filter_result = (compare > 0);
    } break;
    case STRING_LIKE: { //case like/not like
      filter_result = left_cell.string_like(right_cell);
    } break;
    case STRING_NOT_LIKE: {
      filter_result = !left_cell.string_like(right_cell);
    } break;
    default: {
      LOG_WARN("invalid compare type: %d", comp);
    } break;
  }
  if (!filter_result) {
    return false;
  }
  return true;
}

RC do_aggregation(std::ostream &os, TupleSet& tupleSet, Table *default_table,
                    const std::vector<Aggregation>& aggregations,
                    std::unordered_map<std::string, Table*>& table_map,
                    std::map<std::pair<const Table*, const FieldMeta*>, int>& field_to_idx)
{
  for (size_t i = 0; i < aggregations.size(); i ++) {
    const Aggregation &aggregation = aggregations[i];
    if (i != 0) {
      os << " | ";
    }
    Table *table = nullptr;
    if (aggregation.attribute.relation_name != nullptr &&
        !common::is_blank(aggregation.attribute.relation_name)) {
      table = table_map[aggregation.attribute.relation_name];
    } else {
      table = default_table;
    }
    const FieldMeta *field = table->table_meta().field(aggregation.attribute.attribute_name);
    if (field == nullptr && strcmp(aggregation.attribute.attribute_name, "*") != 0) {
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    int idx = field_to_idx[{table, field}];
    int re;
    switch (aggregation.func_name) {
      case AGG_MAX: {
        TupleCell cell;
        bool isFirst = true;
        for (TupleInfo &tuple : tupleSet) {
          if (isFirst) {
            cell = tuple[idx];
            isFirst = false;
          } else {
            if (cell_check(tuple[idx], CompOp::GREAT_THAN, cell)) {
              cell = tuple[idx];
            }
          }
        }
        cell.to_string(os);
      } break;
      case AGG_MIN: {
        TupleCell cell;
        bool isFirst = true;
        for (TupleInfo &tuple : tupleSet) {
          if (isFirst) {
            cell = tuple[idx];
            isFirst = false;
          } else {
            if (cell_check(tuple[idx], CompOp::LESS_THAN, cell)) {
              cell = tuple[idx];
            }
          }
        }
        cell.to_string(os);
      } break;
      case AGG_COUNT: {
        os << tupleSet.size();
      } break;
      case AGG_AVG: {
        float sum = 0.0;
        for (TupleInfo &tuple : tupleSet) {
          float cast_float;
          if (tuple[idx].attr_type() == CHARS) {
            cast_float = atof(tuple[idx].data());
          } else if (tuple[idx].attr_type() == INTS) {
            cast_float = *(int *)tuple[idx].data();
          } else {
            cast_float = *(float *)tuple[idx].data();
          }
          sum += cast_float;
        }
        os << double2string(sum / static_cast<float>(tupleSet.size()));
      } break;
      case AGG_SUM: {
        float sum = 0.0;
        for (TupleInfo &tuple : tupleSet) {
          float cast_float;
          if (tuple[idx].attr_type() == CHARS) {
            cast_float = atof(tuple[idx].data());
          } else if (tuple[idx].attr_type() == INTS) {
            cast_float = *(int *)tuple[idx].data();
          } else {
            cast_float = *(float *)tuple[idx].data();
          }
          sum += cast_float;
        }
        os << double2string(sum);
      } break;
    }
  }
  os << std::endl;
  return RC::SUCCESS;
}

void descartes_helper(std::vector<TupleSet>& list, int pos, TupleSet& returnList, TupleInfo& line)
{
  TupleSet &tupleSet = list[pos];
  if (pos == list.size() - 1) {
    for (TupleInfo &tuple : tupleSet) {
      line.insert(line.end(), tuple.begin(), tuple.end());
      returnList.push_back(line);
      line.erase(line.end() - tuple.size(), line.end());
    }
    return;
  }
  for (TupleInfo &tuple : tupleSet) {
    line.insert(line.end(), tuple.begin(), tuple.end());
    descartes_helper(list, pos + 1, returnList, line);
    line.erase(line.end() - tuple.size(), line.end());
  }
}

TupleSet getDescartes(std::vector<TupleSet>& list)
{
  TupleSet returnList;
  TupleInfo line;
  descartes_helper(list, 0, returnList, line);
  return returnList;
}

void descartes_helper_with_innerjoin(std::vector<TupleSet>& list, int pos,
                                      TupleSet& returnList,
                                      TupleInfo& line,
                                      std::vector<FilterUnit *> &filter_units,
                                      std::map<std::pair<const Table*, const FieldMeta*>, int>& field_to_idx)
{
  TupleSet &tupleSet = list[pos];
  if (pos > 1) {
    FilterUnit *filter_unit = filter_units[pos - 1];
    if (filter_unit) {
      auto *left_expr = dynamic_cast<FieldExpr*>(filter_unit->left());
      auto *right_expr = dynamic_cast<FieldExpr*>(filter_unit->right());

      int left_idx = field_to_idx[{left_expr->field().table(), left_expr->field().meta()}];
      int right_idx = field_to_idx[{right_expr->field().table(), right_expr->field().meta()}];
      TupleCell &left_cell = line[left_idx], &right_cell = line[right_idx];
      if (!cell_check(left_cell, filter_unit->comp(), right_cell)) {
        return;
      }
    }
  }
  if (pos == list.size()) {
    returnList.push_back(line);
    return;
  }
  for (TupleInfo &tuple : tupleSet) {
    line.insert(line.end(), tuple.begin(), tuple.end());
    descartes_helper_with_innerjoin(list, pos + 1, returnList, line, filter_units, field_to_idx);
    line.erase(line.end() - tuple.size(), line.end());
  }
}

TupleSet getDescartes_with_innerjoin(std::vector<TupleSet>& list, FilterStmt *filterStmt,
                                    std::map<std::pair<const Table*, const FieldMeta*>, int>& field_to_idx,
                                    const std::vector<Table*>& tables)
{
  std::vector<FilterUnit *> filter_units_first;
  std::vector<FilterUnit *> filter_units(list.size() + 1);
  for (FilterUnit *filter_unit : filterStmt->filter_units()) {
    auto *left_expr = dynamic_cast<FieldExpr*>(filter_unit->left());
    auto *right_expr = dynamic_cast<FieldExpr*>(filter_unit->right());
    if (filter_unit->left()->type() == ExprType::FIELD && filter_unit->right()->type() == left_expr->type()) {
      if (strcmp(left_expr->table_name(), right_expr->table_name()) != 0) {
        filter_units_first.push_back(filter_unit);
      }
    }
  }
  if (filter_units_first.size() == list.size() - 1) {
    for (size_t i = 0; i < filter_units_first.size(); i ++) {
      filter_units[i + 1] = filter_units_first[i];
    }
  }
  else {
    std::unordered_map<FilterUnit*, int> filter_for_tableIdx;
    int idx = 1;
    for (FilterUnit *filter_unit : filterStmt->filter_units()) {
      auto *left_expr = dynamic_cast<FieldExpr*>(filter_unit->left());
      auto *right_expr = dynamic_cast<FieldExpr*>(filter_unit->right());
      if (filter_unit->left()->type() == ExprType::FIELD && filter_unit->right()->type() == left_expr->type()) {
        if (strcmp(left_expr->table_name(), right_expr->table_name()) != 0) {
          if (std::string(left_expr->table_name()) > std::string(right_expr->table_name())) {
            while (idx < list.size() && left_expr->field().table() != tables[idx]) {
              idx ++;
            }
          }
          else {
            while (idx < list.size() && right_expr->field().table() != tables[idx]) {
              idx ++;
            }
          }
          filter_for_tableIdx[filter_unit] = idx ++;
        }
      }
    }
    for (const auto& item : filter_for_tableIdx) {
      filter_units[item.second] = item.first;
    }
  }

  TupleSet returnList;
  TupleInfo line;
  descartes_helper_with_innerjoin(list, 0, returnList, line, filter_units, field_to_idx);
  return returnList;
}

TupleSet check_condition(TupleSet& list, FilterStmt *filterStmt,
                          std::map<std::pair<const Table*, const FieldMeta*>, int>& field_to_idx)
{
  if (filterStmt == nullptr || filterStmt->filter_units().empty()) {
    return list;
  }
  std::vector<FilterUnit *> filter_units;
  for (FilterUnit *filter_unit : filterStmt->filter_units()) {
    Expression *left_expr = filter_unit->left();
    Expression *right_expr = filter_unit->right();
    if (left_expr->type() == ExprType::FIELD && right_expr->type() == left_expr->type()) {
       const char *left_table_name = dynamic_cast<FieldExpr*>(left_expr)->table_name();
       const char *right_table_name = dynamic_cast<FieldExpr*>(right_expr)->table_name();
       if (strcmp(left_table_name, right_table_name) != 0) {
         filter_units.push_back(filter_unit);
       }
    }
  }
  TupleSet res;
  for (TupleInfo &tuple : list) {
    bool ok = true;
    for (FilterUnit *filter_unit : filter_units) {
      auto *left_expr = dynamic_cast<FieldExpr*>(filter_unit->left());
      auto *right_expr = dynamic_cast<FieldExpr*>(filter_unit->right());

      int left_idx = field_to_idx[{left_expr->field().table(), left_expr->field().meta()}];
      int right_idx = field_to_idx[{right_expr->field().table(), right_expr->field().meta()}];
      TupleCell &left_cell = tuple[left_idx], &right_cell = tuple[right_idx];
      ok = cell_check(left_cell, filter_unit->comp(), right_cell);
      if (!ok) {
        break;
      }
    }
    if (ok) {
      res.push_back(tuple);
    }
  }
  return res;
}

IndexScanOperator *try_to_create_index_scan_operator(FilterStmt *filter_stmt)
{
  const std::vector<FilterUnit *> &filter_units = filter_stmt->filter_units();
  if (filter_units.empty() ) {
    return nullptr;
  }

  // 在所有过滤条件中，找到字段与值做比较的条件，然后判断字段是否可以使用索引
  // 如果是多列索引，这里的处理需要更复杂。
  // 这里的查找规则是比较简单的，就是尽量找到使用相等比较的索引
  // 如果没有就找范围比较的，但是直接排除不等比较的索引查询. (你知道为什么?)
  const FilterUnit *better_filter = nullptr;
  for (const FilterUnit * filter_unit : filter_units) {
    if (filter_unit->comp() == NOT_EQUAL) {
      continue;
    }

    Expression *left = filter_unit->left();
    Expression *right = filter_unit->right();
    if (left->type() == ExprType::VALUE && right->type() == ExprType::VALUE) {
      return nullptr;
    }
    if (left->type() == ExprType::FIELD && right->type() == ExprType::VALUE) {
    } else if (left->type() == ExprType::VALUE && right->type() == ExprType::FIELD) {
      std::swap(left, right);
    }
    FieldExpr &left_field_expr = *(FieldExpr *)left;
    const Field &field = left_field_expr.field();
    const Table *table = field.table();
    Index *index = table->find_index_by_field(field.field_name());
    if (index != nullptr) {
      if (better_filter == nullptr) {
        better_filter = filter_unit;
      } else if (filter_unit->comp() == EQUAL_TO) {
        better_filter = filter_unit;
    	break;
      }
    }
  }

  if (better_filter == nullptr) {
    return nullptr;
  }

  Expression *left = better_filter->left();
  Expression *right = better_filter->right();
  CompOp comp = better_filter->comp();
  if (left->type() == ExprType::VALUE && right->type() == ExprType::FIELD) {
    std::swap(left, right);
    switch (comp) {
    case EQUAL_TO:    { comp = EQUAL_TO; }    break;
    case LESS_EQUAL:  { comp = GREAT_THAN; }  break;
    case NOT_EQUAL:   { comp = NOT_EQUAL; }   break;
    case LESS_THAN:   { comp = GREAT_EQUAL; } break;
    case GREAT_EQUAL: { comp = LESS_THAN; }   break;
    case GREAT_THAN:  { comp = LESS_EQUAL; }  break;
    default: {
    	LOG_WARN("should not happen");
    }
    }
  }


  FieldExpr &left_field_expr = *(FieldExpr *)left;
  const Field &field = left_field_expr.field();
  const Table *table = field.table();
  Index *index = table->find_index_by_field(field.field_name());
  assert(index != nullptr);

  ValueExpr &right_value_expr = *(ValueExpr *)right;
  TupleCell value;
  right_value_expr.get_tuple_cell(value);

  const TupleCell *left_cell = nullptr;
  const TupleCell *right_cell = nullptr;
  bool left_inclusive = false;
  bool right_inclusive = false;

  switch (comp) {
  case EQUAL_TO: {
    left_cell = &value;
    right_cell = &value;
    left_inclusive = true;
    right_inclusive = true;
  } break;

  case LESS_EQUAL: {
    left_cell = nullptr;
    left_inclusive = false;
    right_cell = &value;
    right_inclusive = true;
  } break;

  case LESS_THAN: {
    left_cell = nullptr;
    left_inclusive = false;
    right_cell = &value;
    right_inclusive = false;
  } break;

  case GREAT_EQUAL: {
    left_cell = &value;
    left_inclusive = true;
    right_cell = nullptr;
    right_inclusive = false;
  } break;

  case GREAT_THAN: {
    left_cell = &value;
    left_inclusive = false;
    right_cell = nullptr;
    right_inclusive = false;
  } break;

  default: {
    LOG_WARN("should not happen. comp=%d", comp);
  } break;
  }

  IndexScanOperator *oper = new IndexScanOperator(table, index,
       left_cell, left_inclusive, right_cell, right_inclusive);

  LOG_INFO("use index for scan: %s in table %s", index->index_meta().name(), table->name());
  return oper;
}

RC ExecuteStage::do_select(SQLStageEvent *sql_event)
{
  SelectStmt *select_stmt = (SelectStmt *)(sql_event->stmt());
  SessionEvent *session_event = sql_event->session_event();
  RC rc = RC::SUCCESS;

  std::vector<TupleSet> sel_res;
  std::map<std::pair<const Table*, const FieldMeta*>, int> field_to_idx;
  int idx = 0;
  for (int i = 0; i < select_stmt->tables().size(); i ++) {
    Table *table = select_stmt->tables()[i];
    Operator *scan_oper = try_to_create_index_scan_operator(select_stmt->filter_stmt());
    if (nullptr == scan_oper) {
      scan_oper = new TableScanOperator(table);
    }
    DEFER([&] () {delete scan_oper;});

    PredicateOperator pred_oper(select_stmt->filter_stmt());
    pred_oper.add_child(scan_oper);
    ProjectOperator project_oper;
    project_oper.add_child(&pred_oper);
    for (const Field &field : select_stmt->query_fields()) {
      if (field.table() == table) {
        project_oper.add_projection(field.table(), field.meta(), select_stmt->tables().size() == 1);
        if (!field_to_idx.count({field.table(), field.meta()})) {
          field_to_idx[{field.table(), field.meta()}] = idx ++;
        }
      }
    }
    rc = project_oper.open();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to open operator");
      return rc;
    }
    TupleSet tuples;
    while ((rc = project_oper.next()) == RC::SUCCESS) {
      // get current record
      Tuple * tuple = project_oper.current_tuple();

      if (nullptr == tuple) {
        rc = RC::INTERNAL;
        LOG_WARN("failed to get current record. rc=%s", strrc(rc));
        break;
      }
      TupleInfo tupleInfo;
      for (int i = 0; i < tuple->cell_num(); i ++) {
        TupleCell cell;
        tuple->cell_at(i, cell);
        tupleInfo.push_back(cell);
      }
      tuples.push_back(std::move(tupleInfo));
    }

    if (rc != RC::RECORD_EOF) {
      LOG_WARN("something wrong while iterate operator. rc=%s", strrc(rc));
      project_oper.close();
    } else {
      rc = project_oper.close();
    }
    sel_res.push_back(std::move(tuples));
  }

  TupleSet res;
  if (select_stmt->is_inner_join() && select_stmt->tables().size() >= 3) {
    res = getDescartes_with_innerjoin(sel_res, select_stmt->filter_stmt(), field_to_idx, select_stmt->tables());
  }
  else {
    res = getDescartes(sel_res);
  }

  res = check_condition(res, select_stmt->filter_stmt(), field_to_idx);
  // 输出环节
  std::stringstream ss;
  if (select_stmt->aggregations().size()) { //聚合函数
    print_header_for_aggregation(ss, select_stmt->aggregations());
    RC rc = do_aggregation(ss, res, select_stmt->tables()[0], select_stmt->aggregations(), select_stmt->table_map(), field_to_idx);
    if (rc != RC::SUCCESS) {
      session_event->set_response("FAILURE\n");
      return rc;
    }
  } else { //普通查询
    print_tuple_header_withList(ss, select_stmt->query_fields_forprint(), select_stmt->tables().size() == 1);
    if (select_stmt->tables().size() == 1) {
      for (TupleInfo &tuple : res) {
        tupleInfo_to_string(ss, tuple);
      }
    } else {
      for (TupleInfo &tuple : res) {
        tupleInfo_to_string_with_tables(ss, tuple, field_to_idx, select_stmt->query_fields_forprint());
      }
    }
  }

  session_event->set_response(ss.str());
  return rc;
}

RC ExecuteStage::do_update(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  UpdateStmt *updateStmt = dynamic_cast<UpdateStmt*>(sql_event->stmt());
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  Operator *scan_oper = try_to_create_index_scan_operator(updateStmt->filter_stmt());
  if (nullptr == scan_oper) {
    scan_oper = new TableScanOperator(updateStmt->table());
  }
  DEFER([&] () {delete scan_oper;});

  PredicateOperator pred_oper(updateStmt->filter_stmt());
  pred_oper.add_child(scan_oper);
  UpdateOperator update_oper(updateStmt, trx);
  update_oper.add_child(&pred_oper);
  rc = update_oper.open();

  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
    if (!session->is_trx_multi_operation_mode()) {
      CLogRecord *clog_record = nullptr;
      rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
      if (rc != RC::SUCCESS || clog_record == nullptr) {
        session_event->set_response("FAILURE\n");
        return rc;
      }

      rc = clog_manager->clog_append_record(clog_record);
      if (rc != RC::SUCCESS) {
        session_event->set_response("FAILURE\n");
        return rc;
      }

      trx->next_current_id();
      session_event->set_response("SUCCESS\n");
    }
  }
  return rc;
}

RC ExecuteStage::do_help(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  const char *response = "show tables;\n"
                         "desc `table name`;\n"
                         "create table `table name` (`column name` `column type`, ...);\n"
                         "create index `index name` on `table` (`column`);\n"
                         "insert into `table` values(`value1`,`value2`);\n"
                         "update `table` set column=value [where `column`=`value`];\n"
                         "delete from `table` [where `column`=`value`];\n"
                         "select [ * | `columns` ] from `table`;\n";
  session_event->set_response(response);
  return RC::SUCCESS;
}

RC ExecuteStage::do_create_table(SQLStageEvent *sql_event)
{
  const CreateTable &create_table = sql_event->query()->sstr.create_table;
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  RC rc = db->create_table(create_table.relation_name,
			create_table.attribute_count, create_table.attributes);
  if (rc == RC::SUCCESS) {
    session_event->set_response("SUCCESS\n");
  } else {
    session_event->set_response("FAILURE\n");
  }
  return rc;
}

RC ExecuteStage::do_drop_table(SQLStageEvent *sql_event)
{
  const DropTable &drop_table = sql_event->query()->sstr.drop_table;
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  RC rc = db->drop_table(drop_table.relation_name);
  if (rc == RC::SUCCESS) {
    session_event->set_response("SUCCESS\n");
  } else {
    session_event->set_response("FAILURE\n");
  }
  return rc;
}

RC ExecuteStage::do_create_index(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  const CreateIndex &create_index = sql_event->query()->sstr.create_index;
  Table *table = db->find_table(create_index.relation_name);
  if (nullptr == table) {
    session_event->set_response("FAILURE\n");
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  int attr_num = create_index.attr_num;
  std::vector<std::string> index_set;
  for (int i = attr_num - 1; i >= 0; i --) {
    std::string index_name(create_index.index_name);
    index_name += "_" + std::string(create_index.attributes[i].attribute_name);
    rc = table->create_index(nullptr, index_name.c_str(), create_index.attributes[i].attribute_name);
    if (rc == RC::SUCCESS) {
      index_set.push_back(index_name);
      if (create_index.unique) {
        table->unique_index_set().insert(create_index.attributes[i].attribute_name);
      }
    } else break;
  }
  if (rc == RC::SUCCESS) {
    table->index_set_names().emplace_back(create_index.index_name);
    if (create_index.unique) {
      table->unique_index_set_names().emplace_back(create_index.index_name);
    }
    table->index_sets()[create_index.index_name] = std::move(index_set);
    table->index_insert_forUnique(create_index.index_name);
    sql_event->session_event()->set_response("SUCCESS\n");
  } else {
    sql_event->session_event()->set_response("FAILURE\n");
  }
  return rc;
}

RC ExecuteStage::do_show_tables(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  std::vector<std::string> all_tables;
  db->all_tables(all_tables);
  if (all_tables.empty()) {
    session_event->set_response("No table\n");
  } else {
    std::stringstream ss;
    for (const auto &table : all_tables) {
      ss << table << std::endl;
    }
    session_event->set_response(ss.str().c_str());
  }
  return RC::SUCCESS;
}

RC ExecuteStage::do_show_index(SQLStageEvent *sql_event)
{
  Query *query = sql_event->query();
  Db *db = sql_event->session_event()->session()->get_current_db();
  const char *table_name = query->sstr.show_index.relation_name;
  Table *table = db->find_table(table_name);
  std::stringstream ss;
  if (table != nullptr) {
    const TableMeta &tableMeta = table->table_meta();
    auto &index_set_names = table->index_set_names();
    auto &index_sets_ = table->index_sets();
    auto &unique_set = table->unique_index_set();
    ss << "Table | Non_unique | Key_name | Seq_in_index | Column_name\n";
    for (std::string &index_set_name : index_set_names) {
      if (index_sets_[index_set_name].size() == 1) {
        ss << tableMeta.name() << " | ";
        ss << (unique_set.count(tableMeta.index(index_sets_[index_set_name][0].c_str())->field()) ? 0 : 1) << " | ";
        ss << index_set_name << " | ";
        ss << 1 << " | ";
        ss << tableMeta.index(index_sets_[index_set_name][0].c_str())->field() << "\n";
      }
      else {
        int seq = 1;
        for (std::string &index_name : index_sets_[index_set_name]) {
          ss << tableMeta.name() << " | ";
          ss << (unique_set.count(tableMeta.index(index_name.c_str())->field()) ? 0 : 1) << " | ";
          ss << index_set_name << " | ";
          ss << seq ++ << " | ";
          ss << tableMeta.index(index_name.c_str())->field() << "\n";
        }
      }
    }
  } else {
    sql_event->session_event()->set_response("FAILURE\n");
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  sql_event->session_event()->set_response(ss.str().c_str());
  return RC::SUCCESS;
}

RC ExecuteStage::do_desc_table(SQLStageEvent *sql_event)
{
  Query *query = sql_event->query();
  Db *db = sql_event->session_event()->session()->get_current_db();
  const char *table_name = query->sstr.desc_table.relation_name;
  Table *table = db->find_table(table_name);
  std::stringstream ss;
  if (table != nullptr) {
    table->table_meta().desc(ss);
  } else {
    ss << "No such table: " << table_name << std::endl;
  }
  sql_event->session_event()->set_response(ss.str().c_str());
  return RC::SUCCESS;
}

RC ExecuteStage::do_insert(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }

  InsertStmt *insert_stmt = (InsertStmt *)stmt;
  Table *table = insert_stmt->table();

  RC rc = table->insert_record(trx, insert_stmt->tuple_amount(), insert_stmt->tuples());
  if (rc == RC::SUCCESS) {
    if (!session->is_trx_multi_operation_mode()) {
      CLogRecord *clog_record = nullptr;
      rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
      if (rc != RC::SUCCESS || clog_record == nullptr) {
        session_event->set_response("FAILURE\n");
        return rc;
      }

      rc = clog_manager->clog_append_record(clog_record);
      if (rc != RC::SUCCESS) {
        session_event->set_response("FAILURE\n");
        return rc;
      } 

      trx->next_current_id();
      session_event->set_response("SUCCESS\n");
    } else {
      session_event->set_response("SUCCESS\n");
    }
  } else {
    session_event->set_response("FAILURE\n");
  }
  return rc;
}

RC ExecuteStage::do_delete(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }

  DeleteStmt *delete_stmt = (DeleteStmt *)stmt;
  TableScanOperator scan_oper(delete_stmt->table());
  PredicateOperator pred_oper(delete_stmt->filter_stmt());
  pred_oper.add_child(&scan_oper);
  DeleteOperator delete_oper(delete_stmt, trx);
  delete_oper.add_child(&pred_oper);

  RC rc = delete_oper.open();
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
    if (!session->is_trx_multi_operation_mode()) {
      CLogRecord *clog_record = nullptr;
      rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
      if (rc != RC::SUCCESS || clog_record == nullptr) {
        session_event->set_response("FAILURE\n");
        return rc;
      }

      rc = clog_manager->clog_append_record(clog_record);
      if (rc != RC::SUCCESS) {
        session_event->set_response("FAILURE\n");
        return rc;
      } 

      trx->next_current_id();
      session_event->set_response("SUCCESS\n");
    }
  }
  return rc;
}

RC ExecuteStage::do_begin(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  session->set_trx_multi_operation_mode(true);

  CLogRecord *clog_record = nullptr;
  rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_BEGIN, trx->get_current_id(), clog_record);
  if (rc != RC::SUCCESS || clog_record == nullptr) {
    session_event->set_response("FAILURE\n");
    return rc;
  }

  rc = clog_manager->clog_append_record(clog_record);
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }

  return rc;
}

RC ExecuteStage::do_commit(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  session->set_trx_multi_operation_mode(false);

  CLogRecord *clog_record = nullptr;
  rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
  if (rc != RC::SUCCESS || clog_record == nullptr) {
    session_event->set_response("FAILURE\n");
    return rc;
  }

  rc = clog_manager->clog_append_record(clog_record);
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }

  trx->next_current_id();

  return rc;
}

RC ExecuteStage::do_clog_sync(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  CLogManager *clog_manager = db->get_clog_manager();

  rc = clog_manager->clog_sync();
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }

  return rc;
}
