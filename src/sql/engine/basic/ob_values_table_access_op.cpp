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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_values_table_access_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_obj_cast_runtime.h"
#include <strings.h>

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER_INHERIT(ObValuesTableAccessSpec,
                            ObOpSpec,
                            access_type_,
                            column_exprs_,
                            value_exprs_,
                            start_param_idx_,
                            end_param_idx_,
                            obj_params_);

ObValuesTableAccessOp::ObValuesTableAccessOp(ObExecContext &exec_ctx,
                                             const ObOpSpec &spec,
                                             ObOpInput *input)
  : ObOperator(exec_ctx, spec, input), datum_caster_(), cm_(CM_NONE),
    row_idx_(0), row_cnt_(0), file_reader_(), file_fields_(),
    file_opened_(false), file_has_header_(true), file_null_str_() {}

int ObValuesTableAccessOp::inner_open()
{
  int ret = OB_SUCCESS;
  row_idx_ = 0;
  const int32_t result_flag = 0;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(ctx_.get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(MY_SPEC.column_exprs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(datum_caster_.init(eval_ctx_.exec_ctx_))) {
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, GET_MY_SESSION(ctx_), cm_))) {
  } else {
    cm_ = cm_ | CM_COLUMN_CONVERT;
    if (OB_SUCC(ret)) {
      if (ObValuesTableDef::FILE_ACCESS == MY_SPEC.access_type_) {
        if (OB_FAIL(open_file())) {
          LOG_WARN("failed to open file for file table", K(ret));
        }
      } else if (ObValuesTableDef::FOLD_ACCESS_EXPR == MY_SPEC.access_type_) {
        if (OB_UNLIKELY(MY_SPEC.start_param_idx_ < 0 ||
                        MY_SPEC.start_param_idx_ > MY_SPEC.end_param_idx_ ||
                        MY_SPEC.end_param_idx_ >= plan_ctx->get_param_store().count())) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const ObObj &first_column_node = plan_ctx->get_param_store().at(MY_SPEC.start_param_idx_);
          if (OB_FAIL(first_column_node.get_real_param_count(row_cnt_))) {
          }
        }
      } else if (ObValuesTableDef::ACCESS_PARAM == MY_SPEC.access_type_) {
        int64_t column_cnt = MY_SPEC.column_exprs_.count();
        row_cnt_ = (MY_SPEC.end_param_idx_ - MY_SPEC.start_param_idx_ + 1) / column_cnt;
      } else if (ObValuesTableDef::ACCESS_OBJ == MY_SPEC.access_type_) {
        int64_t column_cnt = MY_SPEC.column_exprs_.count();
        row_cnt_ = MY_SPEC.obj_params_.count() / column_cnt;
      } else {
        int64_t column_cnt = MY_SPEC.column_exprs_.count();
        row_cnt_ = MY_SPEC.value_exprs_.count() / column_cnt;
      }
    }
  }
  return ret;
}

int ObValuesTableAccessOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else if (ObValuesTableDef::FILE_ACCESS == MY_SPEC.access_type_ && file_opened_) {
    if (OB_FAIL(file_reader_.rewind())) {
      LOG_WARN("failed to rewind file", K(ret));
    } else if (file_has_header_ && OB_FAIL(skip_header())) {
      LOG_WARN("failed to skip csv header", K(ret));
    } else {
      row_idx_ = 0;
    }
  } else {
    row_idx_ = 0;
  }
  return ret;
}

int ObValuesTableAccessOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
  } else if (ObValuesTableDef::FILE_ACCESS == MY_SPEC.access_type_ && file_opened_) {
    if (OB_FAIL(file_reader_.rewind())) {
      LOG_WARN("failed to rewind file", K(ret));
    } else if (file_has_header_ && OB_FAIL(skip_header())) {
      LOG_WARN("failed to skip csv header", K(ret));
    } else {
      row_idx_ = 0;
    }
  } else {
    row_idx_ = 0;
  }
  return ret;
}

int ObValuesTableAccessOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_check_status())) {
  } else {
    do {
      clear_evaluated_flag();
      if (OB_FAIL(calc_next_row())) {
        if(OB_ITER_END != ret) {
        }
      } else {
        LOG_DEBUG("output row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
      }
      break;
    } while (OB_SUCC(ret));
  }
  return ret;
}

void ObValuesTableAccessOp::update_src_meta(const ObObjMeta &src_obj_meta,
                                            const ObAccuracy &src_obj_acc,
                                            ObDatumMeta &src_meta)
{
  src_meta.type_ = src_obj_meta.get_type();
  src_meta.cs_type_ = src_obj_meta.get_collation_type();
  src_meta.scale_ = src_obj_acc.get_scale();
  src_meta.precision_ = src_obj_acc.get_precision();
}

int ObValuesTableAccessOp::get_real_src_obj_type(const int64_t row_idx,
                                                 ObExpr &src_expr,
                                                 ObDatumMeta &src_meta,
                                                 ObObjMeta &src_obj_meta)
{
  int ret = OB_SUCCESS;
  if (T_QUESTIONMARK == src_expr.type_) {
    int64_t param_idx = src_expr.extra_;
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    const ObSqlArrayObj *array_obj = NULL;
    if (OB_UNLIKELY(param_idx < 0 || param_idx >= plan_ctx->get_param_store().count())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (plan_ctx->get_param_store().at(param_idx).is_ext_sql_array()) {
      // If the parameter is is_ext_sql_array
      if (OB_ISNULL(array_obj = reinterpret_cast<const ObSqlArrayObj*>(
                                            plan_ctx->get_param_store().at(param_idx).get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (array_obj->count_ <= row_idx) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        src_obj_meta = array_obj->data_[row_idx].meta_;
        const ObAccuracy &src_obj_acc = array_obj->data_[row_idx].get_accuracy();
        update_src_meta(src_obj_meta, src_obj_acc, src_meta);
      }
    } else if (src_expr.frame_idx_ < spec_.plan_->get_expr_frame_info().const_frame_.count() +
                                     spec_.plan_->get_expr_frame_info().param_frame_.count()) {
      src_obj_meta = plan_ctx->get_param_store().at(param_idx).meta_;
      const ObAccuracy &src_obj_acc = plan_ctx->get_param_store().at(param_idx).get_accuracy();
      update_src_meta(src_obj_meta, src_obj_acc, src_meta);
    }
  }
  return ret;
}

int ObValuesTableAccessOp::calc_datum_from_expr(const int64_t col_idx,
                                                const int64_t row_idx,
                                                ObExpr *src_expr,
                                                ObExpr *dst_expr)
{
  int ret = OB_SUCCESS;
  ObDatumMeta src_meta = src_expr->datum_meta_;
  ObObjMeta src_obj_meta = src_expr->obj_meta_;
  ObDatum *datum = NULL;
  bool need_adjust_decimal_int = false;
  if (src_expr == dst_expr) {
    /* actually should not happened */
  } else if (T_QUESTIONMARK == src_expr->type_ &&
             OB_FAIL(get_real_src_obj_type(row_idx, *src_expr, src_meta, src_obj_meta))) {
    LOG_WARN("failed to get real obj type");
  } else {
    need_adjust_decimal_int = src_meta.type_ == ObDecimalIntType &&
                              dst_expr->datum_meta_.type_ == ObDecimalIntType &&
                              ObDatumCast::need_scale_decimalint(src_meta.scale_,
                                                                 src_meta.precision_,
                                                                 dst_expr->datum_meta_.scale_,
                                                                 dst_expr->datum_meta_.precision_);
  }
  if (OB_FAIL(ret)) {
  } else if (src_meta.type_ == dst_expr->datum_meta_.type_ &&
             src_meta.cs_type_ == dst_expr->datum_meta_.cs_type_ &&
             !need_adjust_decimal_int) {
    // when type and collation_type is same. src has the same type to dst;
    if (OB_FAIL(src_expr->eval(eval_ctx_, datum))) {
    } else {
      dst_expr->locate_datum_for_write(eval_ctx_) = *datum;
      dst_expr->set_evaluated_projected(eval_ctx_);
    }
  } else {
    if (OB_FAIL(ObCharset::check_valid_implicit_convert(src_meta.cs_type_,
                                                        dst_expr->datum_meta_.cs_type_))) {
    } else {
      ObExpr real_src_expr = *src_expr;
      real_src_expr.datum_meta_ = src_meta;
      real_src_expr.obj_meta_ = src_obj_meta;
      real_src_expr.obj_datum_map_ = ObDatum::get_obj_datum_map_type(src_meta.type_);
      if (OB_FAIL(datum_caster_.to_type(dst_expr->datum_meta_, real_src_expr, cm_, datum))) {
      } else {
        ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
        if (ObObjDatumMapType::OBJ_DATUM_STRING == dst_expr->obj_datum_map_) {
          ObExprStrResAlloc res_alloc(*dst_expr, eval_ctx_);
          if (OB_FAIL(dst_datum.deep_copy(*datum, res_alloc))) {
          }
        } else {
          ObDataBuffer res_alloc(const_cast<char*>(dst_datum.ptr_), dst_expr->res_buf_len_);
          if (OB_FAIL(dst_datum.deep_copy(*datum, res_alloc))) {
          }
        }
        dst_expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObValuesTableAccessOp::calc_datum_from_param(const ObObj &src_obj, ObExpr *dst_expr)
{
  int ret = OB_SUCCESS;
  ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
  const ObObjType &src_type = src_obj.get_type();
  const ObObjType &dst_type =  dst_expr->obj_meta_.get_type();
  const ObObjParam src_obj_param = static_cast<const ObObjParam>(src_obj);
  bool need_adjust_decimal_int = ob_is_decimal_int_tc(src_type)
                                 && ob_is_decimal_int_tc(dst_expr->datum_meta_.type_)
                                 && ObDatumCast::need_scale_decimalint(src_obj_param.get_scale(),
                                                                       src_obj_param.get_precision(),
                                                                       dst_expr->datum_meta_.scale_,
                                                                       dst_expr->datum_meta_.precision_);
  if (ob_is_decimal_int_tc(src_type) && (ob_is_number_tc(dst_type) || need_adjust_decimal_int)) {
    ObObj dst_obj;
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(GET_MY_SESSION(ctx_));
    ObAccuracy dst_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][dst_type];
    if (ob_is_decimal_int_tc(dst_type)) {
      dst_accuracy.set_scale(dst_expr->datum_meta_.scale_);
      dst_accuracy.set_precision(dst_expr->datum_meta_.precision_);
    }
    ObCastCtx cast_ctx(&eval_ctx_.exec_ctx_.get_allocator(), &dtc_params, cm_, dst_expr->obj_meta_.get_collation_type(), &dst_accuracy);
    ObSqlObjCastRuntime cast_runtime(&eval_ctx_.exec_ctx_);
    cast_ctx.runtime_ = &cast_runtime;
    if (OB_FAIL(ObObjCaster::to_type(dst_type, dst_expr->obj_meta_.get_collation_type(), cast_ctx,
                                     src_obj, dst_obj))) {
    } else if (OB_FAIL(dst_datum.from_obj(dst_obj))) {
    } else {
      dst_expr->set_evaluated_projected(eval_ctx_);
    }
  } else if (OB_FAIL(dst_datum.from_obj(src_obj))) {
  } else {
    dst_expr->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

int ObValuesTableAccessOp::open_file()
{
  int ret = OB_SUCCESS;
  file_opened_ = false;
  file_has_header_ = true;
  file_null_str_.reset();
  char *nul_path = NULL;
  if (OB_UNLIKELY(MY_SPEC.obj_params_.count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file table params", K(ret), K(MY_SPEC.obj_params_.count()));
  } else {
    const ObObj &path_obj = MY_SPEC.obj_params_.at(0);
    ObString path = path_obj.get_varchar();
    if (OB_UNLIKELY(path.empty())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (MY_SPEC.obj_params_.count() > 1) {
      file_has_header_ = (0 != MY_SPEC.obj_params_.at(1).get_tinyint());
    }
    if (OB_FAIL(ret)) {
    } else if (MY_SPEC.obj_params_.count() > 2) {
      file_reader_.set_delimiter(static_cast<char>(MY_SPEC.obj_params_.at(2).get_int()));
    }
    // ensure the file path is nul-terminated before calling fopen()
    if (OB_SUCC(ret) && !path.empty()) {
      if (OB_ISNULL(nul_path = static_cast<char *>(::malloc(path.length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        memcpy(nul_path, path.ptr(), path.length());
        nul_path[path.length()] = '\0';
      }
    }
    if (OB_SUCC(ret) && MY_SPEC.obj_params_.count() > 3) {
      file_reader_.set_quote(static_cast<char>(MY_SPEC.obj_params_.at(3).get_int()));
    }
    if (OB_SUCC(ret) && MY_SPEC.obj_params_.count() > 4) {
      file_reader_.set_escape(static_cast<char>(MY_SPEC.obj_params_.at(4).get_int()));
    }
    if (OB_SUCC(ret) && MY_SPEC.obj_params_.count() > 5) {
      file_null_str_ = MY_SPEC.obj_params_.at(5).get_varchar();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(file_reader_.open(ObString(path.length(), nul_path)))) {
      LOG_WARN("failed to open file", K(ret), K(path));
    } else if (FALSE_IT(file_opened_ = true)) {
    } else if (file_has_header_ && OB_FAIL(skip_header())) {
      LOG_WARN("failed to skip csv header", K(ret));
    } else {
      // file row count is unknown until the file is fully scanned
      row_cnt_ = -1;
    }
  }
  if (NULL != nul_path) {
    ::free(nul_path);
    nul_path = NULL;
  }
  if (OB_FAIL(ret)) {
    file_reader_.close();
    file_opened_ = false;
  }
  return ret;
}

int ObValuesTableAccessOp::skip_header()
{
  int ret = OB_SUCCESS;
  bool has_row = false;
  if (OB_FAIL(file_reader_.next_record(file_fields_, has_row))) {
  } else {
    file_fields_.reuse();
  }
  return ret;
}

int ObValuesTableAccessOp::cast_file_field(const common::ObString &field,
                                           bool is_null,
                                           ObExpr *dst_expr)
{
  int ret = OB_SUCCESS;
  LOG_INFO("cast step enter", K(reinterpret_cast<uint64_t>(dst_expr)), K(dst_expr->frame_idx_),
           K(dst_expr->type_), K(dst_expr->datum_meta_.type_));
  ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
  LOG_INFO("cast step locate ok");
  if (is_null) {
    dst_datum.set_null();
    dst_expr->set_evaluated_projected(eval_ctx_);
  } else if (ObTinyIntType == dst_expr->datum_meta_.type_ &&
             0 == strncasecmp(field.ptr(), "true", 4) && 4 == field.length()) {
    dst_datum.set_int(1);
    dst_expr->set_evaluated_projected(eval_ctx_);
  } else if (ObTinyIntType == dst_expr->datum_meta_.type_ &&
             0 == strncasecmp(field.ptr(), "false", 5) && 5 == field.length()) {
    dst_datum.set_int(0);
    dst_expr->set_evaluated_projected(eval_ctx_);
  } else {
    ObObj src_obj;
    src_obj.set_varchar(field);
    // numeric/temporal columns carry no valid collation, and an invalid collation
    // makes the string-to-number cast dereference a null charset handler.
    ObCollationType src_cs = dst_expr->datum_meta_.cs_type_;
    if (!ObCharset::is_valid_collation(src_cs)) {
      src_cs = CS_TYPE_UTF8MB4_GENERAL_CI;
    }
    src_obj.set_collation_type(src_cs);
    src_obj.set_collation_level(CS_LEVEL_IMPLICIT);
    ObObj dst_obj;
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(GET_MY_SESSION(ctx_));
    ObAccuracy dst_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][dst_expr->datum_meta_.type_];
    if (ob_is_string_tc(dst_expr->datum_meta_.type_)) {
      dst_accuracy.set_length(dst_expr->datum_meta_.precision_);
    } else if (ob_is_enum_or_set_type(dst_expr->datum_meta_.type_)) {
      ret = OB_NOT_SUPPORTED;
    }
    if (OB_FAIL(ret)) {
    } else {
      ObCastCtx cast_ctx(&eval_ctx_.exec_ctx_.get_allocator(),
                         &dtc_params,
                         cm_,
                         dst_expr->datum_meta_.cs_type_,
                         &dst_accuracy);
      ObSqlObjCastRuntime cast_runtime(&eval_ctx_.exec_ctx_);
      cast_ctx.runtime_ = &cast_runtime;
      LOG_INFO("cast step before to_type", K(dst_accuracy.get_precision()), K(cm_));
      if (OB_FAIL(ObObjCaster::to_type(dst_expr->datum_meta_.type_,
                                       dst_expr->datum_meta_.cs_type_,
                                       cast_ctx,
                                       src_obj,
                                       dst_obj))) {
      } else if (dst_obj.is_null()) {
        dst_datum.set_null();
      } else {
        LOG_INFO("cast step to_type ok");
        if (ObObjDatumMapType::OBJ_DATUM_STRING == dst_expr->obj_datum_map_) {
          ObDatum src_datum;
          src_datum.from_obj(dst_obj);
          ObExprStrResAlloc res_alloc(*dst_expr, eval_ctx_);
          LOG_INFO("cast step string deep copy");
          if (OB_FAIL(dst_datum.deep_copy(src_datum, res_alloc))) {
          }
        } else {
          // numeric/temporal datums are stored inline in the frame buffer;
          // from_obj writes through datum.ptr_, so it must be the frame datum.
          if (OB_FAIL(dst_datum.from_obj(dst_obj))) {
          }
        }
        LOG_INFO("cast step copy done", K(ret));
        dst_expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObValuesTableAccessOp::calc_next_row_from_file()
{
  int ret = OB_SUCCESS;
  bool has_row = false;
  file_fields_.reuse();
  if (OB_FAIL(file_reader_.next_record(file_fields_, has_row))) {
  } else if (!has_row) {
    ret = OB_ITER_END;
  } else {
    if (0 == row_idx_) {
      LOG_INFO("file row read", K(file_fields_.count()), K(MY_SPEC.column_exprs_.count()),
               K(MY_SPEC.output_.count()));
    }
    const int64_t col_num = MY_SPEC.column_exprs_.count();
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_num; ++col_idx) {
      ObExpr *col_expr = MY_SPEC.column_exprs_.at(col_idx);
      ObString field;
      if (col_idx < file_fields_.count()) {
        field = file_fields_.at(col_idx);
      }
      bool is_null = field.empty() ||
                     (!file_null_str_.empty() &&
                      field.length() == file_null_str_.length() &&
                      0 == memcmp(field.ptr(), file_null_str_.ptr(), field.length()));
      LOG_INFO("casting file field", K(col_idx), K(field.length()), K(is_null),
               K(col_expr->datum_meta_.type_));
      if (OB_FAIL(cast_file_field(field, is_null, col_expr))) {
      }
      LOG_INFO("cast file field done", K(col_idx), K(ret));
    }
  }
  return ret;
}

OB_INLINE int ObValuesTableAccessOp::calc_next_row()
{
  int ret = OB_SUCCESS;
  int64_t col_num = MY_SPEC.column_exprs_.count();
  int64_t col_idx = 0;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (row_idx_ == row_cnt_) {
    ret = OB_ITER_END;
  } else if (ObValuesTableDef::FOLD_ACCESS_EXPR == MY_SPEC.access_type_ &&
             OB_FAIL(plan_ctx->replace_batch_param_datum(row_idx_,
                                          MY_SPEC.start_param_idx_,
                                          MY_SPEC.end_param_idx_ - MY_SPEC.start_param_idx_ + 1))) {
  } else if (ObValuesTableDef::FILE_ACCESS == MY_SPEC.access_type_) {
    if (OB_FAIL(calc_next_row_from_file())) {
    }
  } else {
    while (OB_SUCC(ret) && col_idx < col_num) {
      ObExpr *col_expr = MY_SPEC.column_exprs_.at(col_idx);
      switch (MY_SPEC.access_type_)
      {
        case ObValuesTableDef::ACCESS_EXPR : {
          int64_t idx = row_idx_ * col_num + col_idx;
          ObExpr *src_expr = MY_SPEC.value_exprs_.at(idx);
          if (OB_FAIL(calc_datum_from_expr(col_idx, row_idx_, src_expr, col_expr))) {
          }
          break;
        }
        case ObValuesTableDef::FOLD_ACCESS_EXPR : {
          ObExpr *src_expr = MY_SPEC.value_exprs_.at(col_idx);
          if (OB_FAIL(calc_datum_from_expr(col_idx, row_idx_, src_expr, col_expr))) {
          }
          break;
        }
        case ObValuesTableDef::ACCESS_PARAM: {
          int64_t param_idx = MY_SPEC.start_param_idx_ + col_idx + row_idx_ * col_num;
          const ObObjParam &param = plan_ctx->get_param_store().at(param_idx);
          if (OB_FAIL(calc_datum_from_param(param, col_expr))) {
          }
          break;
        }
        case ObValuesTableDef::ACCESS_OBJ: {
          int64_t idx = row_idx_ * col_num + col_idx;
          const ObObj &param = MY_SPEC.obj_params_.at(idx);
          if (OB_FAIL(calc_datum_from_param(param, col_expr))) {
          }
          break;
        }
        case ObValuesTableDef::FILE_ACCESS: {
          break;
        }
      };
      col_idx++;
    } // while end
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_.count(); i++) {
    ObExpr *output = MY_SPEC.output_.at(i);
    if (i < col_num && output == MY_SPEC.column_exprs_.at(i)) {
      /* need do nothing */
    } else {
      ObDatum *dst_datum = NULL;
      MY_SPEC.output_.at(i)->eval(eval_ctx_, dst_datum);
    }
  }
  if (OB_SUCC(ret)) {
    ++row_idx_;
  }
  return ret;
}

int ObValuesTableAccessOp::inner_close()
{
  int ret = OB_SUCCESS;
  row_idx_ = 0;
  if (file_opened_) {
    file_reader_.close();
    file_opened_ = false;
  }
  if (OB_FAIL(datum_caster_.destroy())) {
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase
