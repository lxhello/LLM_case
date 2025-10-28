#!/usr/bin/env python3
"""
数据处理模块
处理前端上传的表格数据，转换为评分系统所需的格式
"""

import pandas as pd
import numpy as np
import logging
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from pathlib import Path


class DataProcessor:
    """数据处理器，负责将上传的表格数据转换为评分系统格式"""
    
    def __init__(self):
        """初始化数据处理器"""
        self.logger = logging.getLogger(__name__)
        
        # 字段映射配置
        self.field_mapping = {
            # 必需字段映射
            'id_number': ['身份证号', '身份号码', 'ID'],
            'name': ['姓名', '用户姓名', '客户姓名', '真实姓名'],  # 新增姓名字段映射
            'phone': ['电话号码', '手机号码', '联系电话', '电话', '手机号', '联系方式'],  # 新增电话字段映射
            'withdraw_date': ['发生日期', '交易日期', '日期'],
            'withdraw_time': ['发生时间', '交易时间', '时间'],
            'amount': ['取现金额合计', '取现金额', '金额', '取现合计'],
            'location': ['业务发生地网点名称', '网点名称', '发生地点', '地点'],
            'transaction_count': ['取现次数', '交易次数', '次数'],
            'gender': ['性别', '性别（0女 1男）'],
            'age': ['年龄'],  # 年龄字段映射
            'has_history_warning': ['取现预警（0否 1是）', '预警', '取现预警'],
            'has_history_fraud': ['被骗历史', '被骗史', '诈骗历史'],
            'has_special_comm': ['小众聊天', '小众通联', '特殊聊天'],
            'has_adult_app': ['涉黄软件', '涉黄APP', '成人软件'],
            'warning_count': ['预警次数'],  # 合并后的预警次数字段
            'fraud_type': ['疑似诈骗类型', '诈骗类型', '类型']  # 疑似诈骗类型字段
        }
        
        # 地点关键字映射
        self.location_keywords = {
            '定海': ['定海'],
            '普陀': ['普陀'],
            '岱山': ['岱山'],
            '嵊泗': ['嵊泗'],
            '其他': ['其他', '其它', '未知']
        }
        
        # 设置日志
        logging.basicConfig(level=logging.INFO)
    
    def find_column_by_keywords(self, df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
        """根据关键字找到对应的列名"""
        for keyword in keywords:
            for col in df.columns:
                if keyword in str(col):
                    return col
        return None
    
    def extract_location(self, location_text: str) -> str:
        """从地点文本中提取标准地点名称"""
        if pd.isna(location_text) or not location_text:
            return '其他'
        
        location_str = str(location_text)
        
        # 按优先级匹配地点关键字
        for location, keywords in self.location_keywords.items():
            for keyword in keywords:
                if keyword in location_str:
                    return location
        
        return '其他'
    
    def convert_gender(self, gender_value: Union[str, int, float]) -> str:
        """转换性别值"""
        if pd.isna(gender_value):
            return 'unknown'
        
        # 如果是数字格式
        if isinstance(gender_value, (int, float)):
            return 'female' if gender_value == 0 else 'male'
        
        # 如果是文字格式
        gender_str = str(gender_value).strip()
        if gender_str in ['女', 'F', 'Female', '0']:
            return 'female'
        elif gender_str in ['男', 'M', 'Male', '1']:
            return 'male'
        else:
            return 'unknown'
    
    def convert_boolean_field(self, value: Union[str, int, float]) -> bool:
        """转换布尔字段（0/1、是/否等）"""
        if pd.isna(value):
            return False
        
        # 数字格式
        if isinstance(value, (int, float)):
            return bool(value)
        
        # 文字格式
        value_str = str(value).strip().lower()
        return value_str in ['1', '是', 'true', 'yes', 'y', '有']
    
    def generate_user_name(self, name_value: Any, phone_value: Any, id_number: str) -> str:
        """
        按优先级生成用户显示名称
        1. 如果有姓名和手机号，则全部显示（如：“张三 13812345678”）
        2. 如果只有号码没姓名，则只显示手机号码（如：“13812345678”）
        3. 如果都没有，则采用身份证号后四位（如：“用户2925”）
        """
        # 检查姓名字段
        has_name = False
        name_str = ""
        if name_value is not None and not pd.isna(name_value):
            name_str = str(name_value).strip()
            if name_str and name_str not in ['', 'nan', 'NaN', 'null', 'NULL']:
                has_name = True
        
        # 检查电话号码字段
        has_phone = False
        phone_str = ""
        if phone_value is not None and not pd.isna(phone_value):
            # 如果是浮点数，先转换为整数再转字符串，去除小数点
            if isinstance(phone_value, float):
                phone_str = str(int(phone_value)).strip()
            else:
                phone_str = str(phone_value).strip()
            if phone_str and phone_str not in ['', 'nan', 'NaN', 'null', 'NULL']:
                has_phone = True
        
        # 按优先级返回结果
        if has_name and has_phone:
            # 如果有姓名和手机号，则全部显示
            return f"{name_str} {phone_str}"
        elif has_phone:
            # 如果只有手机号，则只显示手机号码
            return phone_str
        elif has_name:
            # 如果只有姓名，则只显示姓名
            return name_str
        else:
            # 都没有有效数据，使用身份证号后4位
            return f"用户{str(id_number)[-4:]}"
    
    def parse_datetime(self, date_value: Any, time_value: Any = None) -> Optional[datetime]:
        """解析日期时间"""
        try:
            if pd.isna(date_value):
                return None
            
            # 如果只有日期，没有时间
            if time_value is None or pd.isna(time_value):
                if isinstance(date_value, datetime):
                    return date_value
                return pd.to_datetime(date_value)
            
            # 组合日期和时间
            date_str = str(date_value)
            time_str = str(time_value)
            
            # 尝试不同的日期时间格式
            datetime_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%Y/%m/%d %H:%M',
                '%m/%d/%Y %H:%M:%S',
                '%m/%d/%Y %H:%M'
            ]
            
            datetime_str = f"{date_str} {time_str}"
            
            for fmt in datetime_formats:
                try:
                    return datetime.strptime(datetime_str, fmt)
                except ValueError:
                    continue
            
            # 如果格式都不匹配，使用pandas的智能解析
            return pd.to_datetime(datetime_str)
            
        except Exception as e:
            self.logger.warning(f"日期时间解析失败: {date_value} {time_value}, 错误: {e}")
            return None
    
    def check_file_compatibility(self, file_path: str) -> Dict[str, Any]:
        """检查文件兼容性并返回详细信息"""
        file_info = {
            "file_path": file_path,
            "exists": False,
            "extension": None,
            "size": 0,
            "is_supported": False,
            "supported_formats": ['.csv', '.xlsx', '.xls'],
            "error": None
        }
        
        try:
            file_obj = Path(file_path)
            file_info["exists"] = file_obj.exists()
            
            if file_info["exists"]:
                file_info["extension"] = file_obj.suffix.lower()
                file_info["size"] = file_obj.stat().st_size
                file_info["is_supported"] = file_info["extension"] in file_info["supported_formats"]
            else:
                file_info["error"] = "文件不存在"
                
        except Exception as e:
            file_info["error"] = str(e)
            
        return file_info
    
    def process_uploaded_data(self, file_path: str) -> List[Dict[str, Union[str, datetime, float, int, bool]]]:
        """处理上传的表格文件"""
        # 首先检查文件兼容性
        file_info = self.check_file_compatibility(file_path)
        self.logger.info(f"文件兼容性检查结果: {file_info}")
        
        if not file_info["exists"]:
            raise FileNotFoundError(f"文件不存在: {file_path}")
            
        if not file_info["is_supported"]:
            error_msg = f"不支持的文件格式: {file_info['extension']}。支持的格式: {', '.join(file_info['supported_formats'])}"
            raise ValueError(error_msg)
        
        try:
            # 读取文件
            file_ext = Path(file_path).suffix.lower()
            self.logger.info(f"开始读取文件: {file_path}")
            
            if file_ext == '.csv':
                df = pd.read_csv(file_path, encoding='utf-8-sig')
            elif file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                # 这里不应该发生，因为之前已经检查过了
                raise ValueError(f"未知错误: 文件格式 {file_ext} 已通过检查但仍然无法处理")
            
            self.logger.info(f"成功读取文件，共 {len(df)} 行数据")
            self.logger.info(f"列名: {list(df.columns)}")
            
            # 找到对应的列
            column_map = {}
            for field, keywords in self.field_mapping.items():
                col_name = self.find_column_by_keywords(df, keywords)
                if col_name:
                    column_map[field] = col_name
                    self.logger.info(f"字段映射: {field} -> {col_name}")
                else:
                    self.logger.warning(f"未找到字段: {field}，关键字: {keywords}")
            
            # 转换数据
            processed_data = []
            
            for index, row in df.iterrows():
                try:
                    # 必需的身份证号
                    if 'id_number' not in column_map:
                        self.logger.error("缺少身份证号字段，无法处理数据")
                        continue
                    
                    id_number = row[column_map['id_number']]
                    # 安全的pandas值处理
                    try:
                        id_is_null = pd.isna(id_number)
                        if isinstance(id_is_null, (bool, np.bool_)):
                            is_null = id_is_null
                        else:
                            is_null = bool(id_is_null.any()) if hasattr(id_is_null, 'any') else True
                    except:
                        is_null = True
                    
                    if is_null or str(id_number).strip() == '':
                        try:
                            row_num = int(index) if isinstance(index, (int, float)) else int(str(index))
                        except (ValueError, TypeError):
                            row_num = 0
                        self.logger.warning(f"第{row_num+1}行身份证号为空，跳过")
                        continue
                    
                    # 获取姓名和电话号码用于生成用户名
                    name_value = None
                    phone_value = None
                    
                    if 'name' in column_map:
                        name_value = row[column_map['name']]
                    
                    if 'phone' in column_map:
                        phone_value = row[column_map['phone']]
                    
                    # 构建人员数据
                    person_data: Dict[str, Union[str, datetime, float, int, bool]] = {
                        'id': str(id_number),
                        'id_number': str(id_number),  # 添加完整身份证号码字段
                        'name': self.generate_user_name(name_value, phone_value, str(id_number)),  # 使用新的用户名生成逻辑
                        'account': f"****{str(id_number)[-4:]}",  # 生成账号
                        'status': '成功',
                        'card_type': '储蓄卡',
                        'risk_level': '低'
                    }
                    
                    # 处理日期时间
                    if 'withdraw_date' in column_map:
                        date_col = column_map['withdraw_date']
                        time_col = column_map.get('withdraw_time')
                        
                        if time_col:
                            withdraw_time = self.parse_datetime(
                                row[date_col], row[time_col]
                            )
                            if withdraw_time:
                                person_data['withdraw_time'] = withdraw_time
                        else:
                            withdraw_time = self.parse_datetime(row[date_col])
                            if withdraw_time:
                                person_data['withdraw_time'] = withdraw_time
                    
                    # 处理金额
                    if 'amount' in column_map:
                        amount_value = row[column_map['amount']]
                        # 安全的pandas值处理
                        try:
                            amount_is_null = pd.isna(amount_value)
                            if isinstance(amount_is_null, (bool, np.bool_)):
                                is_null = amount_is_null
                            else:
                                is_null = bool(amount_is_null.any()) if hasattr(amount_is_null, 'any') else True
                        except:
                            is_null = True
                        
                        if not is_null and str(amount_value).strip() != '':
                            try:
                                person_data['amount'] = float(amount_value)
                            except (ValueError, TypeError):
                                person_data['amount'] = 0.0
                        else:
                            person_data['amount'] = 0.0
                    else:
                        person_data['amount'] = 0.0
                    
                    # 处理地点
                    if 'location' in column_map:
                        location_value = row[column_map['location']]
                        # 安全的pandas值处理
                        try:
                            location_is_null = pd.isna(location_value)
                            if isinstance(location_is_null, (bool, np.bool_)):
                                is_null = location_is_null
                            else:
                                is_null = bool(location_is_null.any()) if hasattr(location_is_null, 'any') else True
                        except:
                            is_null = True
                        
                        location_str = str(location_value) if not is_null else ''
                        person_data['location'] = self.extract_location(location_str)
                    else:
                        person_data['location'] = '其他'
                    
                    # 处理性别
                    if 'gender' in column_map:
                        gender_value = row[column_map['gender']]
                        # 安全的pandas值处理
                        try:
                            gender_is_null = pd.isna(gender_value)
                            if isinstance(gender_is_null, (bool, np.bool_)):
                                is_null = gender_is_null
                            else:
                                is_null = bool(gender_is_null.any()) if hasattr(gender_is_null, 'any') else True
                        except:
                            is_null = True
                        
                        if not is_null:
                            # 确保传递标量值
                            try:
                                if hasattr(gender_value, 'item'):
                                    gender_scalar = gender_value.item()
                                elif isinstance(gender_value, (str, int, float)):
                                    gender_scalar = gender_value
                                else:
                                    gender_scalar = str(gender_value)
                                person_data['gender'] = self.convert_gender(gender_scalar)
                            except (ValueError, TypeError, AttributeError):
                                person_data['gender'] = 'unknown'
                        else:
                            person_data['gender'] = 'unknown'
                    else:
                        person_data['gender'] = 'unknown'
                    
                    # 处理年龄字段
                    if 'age' in column_map:
                        age_value = row[column_map['age']]
                        try:
                            age_is_null = pd.isna(age_value)
                            if isinstance(age_is_null, (bool, np.bool_)):
                                is_null = age_is_null
                            else:
                                is_null = bool(age_is_null.any()) if hasattr(age_is_null, 'any') else True
                        except:
                            is_null = True
                        
                        if not is_null and str(age_value).strip() != '':
                            try:
                                person_data['age'] = int(float(age_value))
                            except (ValueError, TypeError):
                                person_data['age'] = None
                        else:
                            person_data['age'] = None
                    else:
                        person_data['age'] = None
                    
                    # 处理交易次数
                    if 'transaction_count' in column_map:
                        count_value = row[column_map['transaction_count']]
                        # 安全的pandas值处理
                        try:
                            count_is_null = pd.isna(count_value)
                            if isinstance(count_is_null, (bool, np.bool_)):
                                is_null = count_is_null
                            else:
                                is_null = bool(count_is_null.any()) if hasattr(count_is_null, 'any') else True
                        except:
                            is_null = True
                        
                        if not is_null and str(count_value).strip() != '':
                            try:
                                person_data['transaction_count'] = int(float(count_value))
                            except (ValueError, TypeError):
                                person_data['transaction_count'] = 1
                        else:
                            person_data['transaction_count'] = 1
                    else:
                        person_data['transaction_count'] = 1
                    
                    # 处理布尔字段
                    boolean_fields = {
                        'has_history_warning': 'has_history_warning',
                        'has_history_fraud': 'has_history_warning',  # 被骗历史也算预警
                        'has_special_comm': 'has_special_comm',
                        'has_adult_app': 'has_adult_app'
                    }
                    
                    # 初始化布尔字段
                    person_data['has_history_warning'] = False
                    person_data['has_special_comm'] = False
                    person_data['has_adult_app'] = False
                    person_data['has_investment_app'] = False
                    
                    for field, target_field in boolean_fields.items():
                        if field in column_map:
                            field_value = row[column_map[field]]
                            # 安全的pandas值处理
                            try:
                                field_is_null = pd.isna(field_value)
                                if isinstance(field_is_null, (bool, np.bool_)):
                                    is_null = field_is_null
                                else:
                                    is_null = bool(field_is_null.any()) if hasattr(field_is_null, 'any') else True
                            except:
                                is_null = True
                            
                            if not is_null:
                                # 确保传递标量值
                                try:
                                    if hasattr(field_value, 'item'):
                                        field_scalar = field_value.item()
                                    elif isinstance(field_value, (str, int, float)):
                                        field_scalar = field_value
                                    else:
                                        field_scalar = str(field_value)
                                    
                                    if field == 'has_history_fraud':
                                        # 被骗历史映射到历史预警
                                        current_warning = person_data.get('has_history_warning', False)
                                        person_data['has_history_warning'] = (
                                            current_warning or 
                                            self.convert_boolean_field(field_scalar)
                                        )
                                    else:
                                        person_data[target_field] = self.convert_boolean_field(field_scalar)
                                except (ValueError, TypeError, AttributeError):
                                    # 如果转换失败，使用默认值
                                    if field != 'has_history_fraud':
                                        person_data[target_field] = False
                    
                    # 处理预警次数字段（从合并后的文件读取）
                    if 'warning_count' in column_map:
                        warning_count_value = row[column_map['warning_count']]
                        try:
                            if pd.isna(warning_count_value):
                                person_data['预警次数'] = 0
                            else:
                                person_data['预警次数'] = int(float(warning_count_value))
                        except (ValueError, TypeError):
                            person_data['预警次数'] = 0
                    else:
                        person_data['预警次数'] = 0
                    
                    # 处理疑似诈骗类型字段（从合并后的文件读取）
                    if 'fraud_type' in column_map:
                        fraud_type_value = row[column_map['fraud_type']]
                        try:
                            if pd.isna(fraud_type_value):
                                person_data['疑似诈骗类型'] = ''
                            else:
                                person_data['疑似诈骗类型'] = str(fraud_type_value).strip()
                        except (ValueError, TypeError):
                            person_data['疑似诈骗类型'] = ''
                    else:
                        person_data['疑似诈骗类型'] = ''
                    
                    processed_data.append(person_data)
                    
                except Exception as e:
                    try:
                        row_num = int(index) if isinstance(index, (int, float)) else int(str(index))
                    except (ValueError, TypeError):
                        row_num = 0
                    self.logger.error(f"处理第{row_num+1}行数据时出错: {e}")
                    continue
            
            self.logger.info(f"成功处理 {len(processed_data)} 条人员数据")
            return processed_data
            
        except Exception as e:
            self.logger.error(f"文件处理失败: {e}")
            self.logger.error(f"文件路径: {file_path}")
            self.logger.error(f"文件是否存在: {Path(file_path).exists()}")
            if Path(file_path).exists():
                self.logger.error(f"文件大小: {Path(file_path).stat().st_size} bytes")
                self.logger.error(f"文件扩展名: {Path(file_path).suffix}")
            raise
    
    def validate_processed_data(self, data: List[Dict[str, Union[str, datetime, float, int, bool]]]) -> bool:
        """验证处理后的数据格式"""
        required_fields = [
            'id', 'name', 'account', 'amount', 'location', 'gender',
            'transaction_count', 'has_history_warning', 'has_special_comm', 
            'has_adult_app', 'has_investment_app'
        ]
        
        for i, person in enumerate(data):
            for field in required_fields:
                if field not in person:
                    self.logger.error(f"第{i+1}条数据缺少字段: {field}")
                    return False
        
        self.logger.info("数据验证通过")
        return True
    
    def read_file(self, file_path: str) -> pd.DataFrame:
        """读取Excel或CSV文件"""
        file_ext = Path(file_path).suffix.lower()
        self.logger.info(f"读取文件: {file_path}, 扩展名: {file_ext}")
        
        if file_ext == '.csv':
            df = pd.read_csv(file_path, encoding='utf-8-sig')
        elif file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"不支持的文件格式: {file_ext}")
        
        self.logger.info(f"成功读取文件，共 {len(df)} 行，列: {list(df.columns)}")
        return df
    
    def save_file(self, df: pd.DataFrame, output_path: str):
        """保存DataFrame到文件"""
        file_ext = Path(output_path).suffix.lower()
        self.logger.info(f"保存文件到: {output_path}, 格式: {file_ext}")
        
        if file_ext == '.csv':
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
        elif file_ext in ['.xlsx', '.xls']:
            df.to_excel(output_path, index=False, engine='openpyxl')
        else:
            raise ValueError(f"不支持的输出文件格式: {file_ext}")
        
        self.logger.info(f"文件已保存: {output_path}")
    
    def merge_warning_count_to_withdraw_records(
        self, 
        warning_file_path: str, 
        withdraw_file_path: str, 
        output_path: str = None
    ) -> pd.DataFrame:
        """
        将预警查询列表中的预警次数合并到取现记录中
        
        参数:
            warning_file_path: 预警查询列表导出文件路径
            withdraw_file_path: 取现记录导出文件路径  
            output_path: 输出文件路径（如果为None则不保存文件）
        
        返回:
            合并后的DataFrame
        """
        try:
            # 读取预警查询列表文件
            self.logger.info(f"读取预警查询列表文件: {warning_file_path}")
            warning_df = self.read_file(warning_file_path)
            
            # 读取取现记录文件
            self.logger.info(f"读取取现记录文件: {withdraw_file_path}")
            withdraw_df = self.read_file(withdraw_file_path)
            
            # 在预警文件中查找"受害人身份证号"和"受害人号码"列
            victim_id_col = self.find_column_by_keywords(warning_df, ['受害人身份证号', '身份证号'])
            victim_phone_col = self.find_column_by_keywords(warning_df, ['受害人号码', '号码'])
            
            if not victim_id_col and not victim_phone_col:
                self.logger.error(f"无法找到预警文件中的'受害人身份证号'或'受害人号码'列，可用列: {list(warning_df.columns)}")
                raise ValueError("无法找到预警文件中的'受害人身份证号'或'受害人号码'列")
            
            # 在取现记录文件中查找"身份证号"和"电话号码"列
            withdraw_id_col = self.find_column_by_keywords(withdraw_df, ['身份证号'])
            withdraw_phone_col = self.find_column_by_keywords(withdraw_df, ['电话号码', '手机号', '电话', '联系方式'])
            
            if not withdraw_id_col and not withdraw_phone_col:
                self.logger.error(f"无法找到取现记录文件中的'身份证号'或'电话号码'列，可用列: {list(withdraw_df.columns)}")
                raise ValueError("无法找到取现记录文件中的'身份证号'或'电话号码'列")
            
            self.logger.info(f"预警文件列 - 身份证号: {victim_id_col}, 号码: {victim_phone_col}")
            self.logger.info(f"取现记录列 - 身份证号: {withdraw_id_col}, 电话号码: {withdraw_phone_col}")
            
            # 初始化合并结果
            merged_df = withdraw_df.copy()
            merged_df['预警次数'] = 0
            
            # 查找"疑似诈骗类型"列
            fraud_type_col = self.find_column_by_keywords(warning_df, ['疑似诈骗类型', '诈骗类型', '类型'])
            
            if fraud_type_col:
                self.logger.info(f"找到疑似诈骗类型列: {fraud_type_col}")
                merged_df['疑似诈骗类型'] = ''
            else:
                self.logger.warning("未找到'疑似诈骗类型'列")
                fraud_type_col = None
                merged_df['疑似诈骗类型'] = ''
            
            id_matched_count = 0
            
            # 第一步：按身份证号匹配
            if victim_id_col and withdraw_id_col:
                self.logger.info("开始使用身份证号进行匹配...")
                
                # 统计每个身份证号的预警次数，同时收集疑似诈骗类型
                id_warning_info = {}
                
                for idx, row in warning_df.iterrows():
                    victim_id = str(row[victim_id_col]).strip() if pd.notna(row[victim_id_col]) else ''
                    if victim_id and victim_id != '' and victim_id != 'nan':
                        if victim_id not in id_warning_info:
                            id_warning_info[victim_id] = {
                                'count': 0,
                                'types': []
                            }
                        id_warning_info[victim_id]['count'] += 1
                        
                        # 收集疑似诈骗类型（保留所有原始值）
                        if fraud_type_col and pd.notna(row[fraud_type_col]):
                            fraud_type = str(row[fraud_type_col]).strip()
                            if fraud_type and fraud_type != '' and fraud_type != 'nan':
                                id_warning_info[victim_id]['types'].append(fraud_type)
                
                # 转换为DataFrame
                id_warning_data = []
                for victim_id, info in id_warning_info.items():
                    # 不去重，保留所有原始值
                    fraud_types = ', '.join(info['types']) if info['types'] else ''
                    id_warning_data.append({
                        '身份证号': victim_id,
                        '预警次数': info['count'],
                        '疑似诈骗类型': fraud_types
                    })
                
                id_counts_df = pd.DataFrame(id_warning_data)
                
                self.logger.info(f"统计到 {len(id_counts_df)} 个不重复的身份证号")
                
                # 标准化取现记录中的身份证号
                withdraw_df['身份证号_clean'] = withdraw_df[withdraw_id_col].astype(str).str.strip()
                
                # 匹配身份证号
                id_matched = withdraw_df.merge(
                    id_counts_df,
                    left_on='身份证号_clean',
                    right_on='身份证号',
                    how='left'
                )
                
                # 填充匹配结果
                merged_df['预警次数'] = id_matched['预警次数'].fillna(0).astype(int)
                merged_df['疑似诈骗类型'] = id_matched.get('疑似诈骗类型', pd.Series([''] * len(merged_df))).fillna('')
                
                # 记录匹配结果
                id_matched_count = int((merged_df['预警次数'] > 0).sum())
                self.logger.info(f"身份证号匹配成功: {id_matched_count} 条记录")
            
            # 第二步：对未匹配的记录，使用手机号匹配
            if victim_phone_col and withdraw_phone_col:
                self.logger.info("开始使用手机号进行匹配...")
                # 找出未匹配的记录（预警次数为0的记录）
                unmatched_df = merged_df[merged_df['预警次数'] == 0].copy()
                
                if len(unmatched_df) > 0:
                    self.logger.info(f"待匹配记录数: {len(unmatched_df)}")
                    
                    # 统计每个手机号的预警次数，同时收集疑似诈骗类型
                    phone_warning_info = {}
                    
                    for idx, row in warning_df.iterrows():
                        victim_phone = str(row[victim_phone_col]).strip() if pd.notna(row[victim_phone_col]) else ''
                        if victim_phone and victim_phone != '' and victim_phone != 'nan':
                            # 只统计数字开头的电话号码
                            if victim_phone and victim_phone[0].isdigit():
                                if victim_phone not in phone_warning_info:
                                    phone_warning_info[victim_phone] = {
                                        'count': 0,
                                        'types': []
                                    }
                                phone_warning_info[victim_phone]['count'] += 1
                                
                                # 收集疑似诈骗类型（保留所有原始值）
                                if fraud_type_col and pd.notna(row[fraud_type_col]):
                                    fraud_type = str(row[fraud_type_col]).strip()
                                    if fraud_type and fraud_type != '' and fraud_type != 'nan':
                                        phone_warning_info[victim_phone]['types'].append(fraud_type)
                    
                    self.logger.info(f"统计到 {len(phone_warning_info)} 个不重复的电话号码")
                    
                    # 更新未匹配记录的预警次数和疑似诈骗类型
                    for orig_idx in unmatched_df.index:
                        phone = str(unmatched_df.loc[orig_idx, withdraw_phone_col]).strip()
                        if phone in phone_warning_info:
                            info = phone_warning_info[phone]
                            merged_df.loc[orig_idx, '预警次数'] = int(info['count'])
                            if fraud_type_col:
                                # 不去重，保留所有原始值
                                fraud_types = ', '.join(info['types']) if info['types'] else ''
                                merged_df.loc[orig_idx, '疑似诈骗类型'] = fraud_types
                    
                    # 记录匹配结果
                    new_matched_count = int((merged_df['预警次数'] > 0).sum())
                    phone_matched_count = new_matched_count - id_matched_count
                    self.logger.info(f"手机号匹配成功: {phone_matched_count} 条记录")
            
            # 清理临时列
            columns_to_drop = []
            if '身份证号_clean' in merged_df.columns:
                columns_to_drop.append('身份证号_clean')
            if '身份证号_x' in merged_df.columns:
                columns_to_drop.append('身份证号_x')
            if '身份证号_y' in merged_df.columns:
                columns_to_drop.append('身份证号_y')
            if '电话号码_clean' in merged_df.columns:
                columns_to_drop.append('电话号码_clean')
            if '电话号码_x' in merged_df.columns:
                columns_to_drop.append('电话号码_x')
            if '电话号码_y' in merged_df.columns:
                columns_to_drop.append('电话号码_y')
            
            if columns_to_drop:
                merged_df = merged_df.drop(columns=columns_to_drop)
            
            self.logger.info(f"合并完成，共 {len(merged_df)} 条记录")
            self.logger.info(f"其中 {len(merged_df[merged_df['预警次数'] > 0])} 条有预警记录")
            
            # 如果指定了输出路径，保存文件
            if output_path:
                self.save_file(merged_df, output_path)
                self.logger.info(f"结果已保存到: {output_path}")
            
            # 返回统计信息
            stats = {
                'total_records': len(merged_df),
                'with_warning': int((merged_df['预警次数'] > 0).sum()),
                'without_warning': int((merged_df['预警次数'] == 0).sum()),
                'max_warning_count': int(merged_df['预警次数'].max()) if len(merged_df) > 0 else 0,
                'avg_warning_count': float(merged_df['预警次数'].mean()) if len(merged_df) > 0 else 0.0
            }
            self.logger.info(f"合并统计: {stats}")
            
            return merged_df
            
        except Exception as e:
            self.logger.error(f"合并预警次数失败: {e}")
            raise


if __name__ == "__main__":
    # 测试数据处理
    processor = DataProcessor()
    
    # 示例：处理测试文件
    # processed_data = processor.process_uploaded_data('test_data.xlsx')
    # if processor.validate_processed_data(processed_data):
    #     print(f"成功处理 {len(processed_data)} 条数据")
    #     for person in processed_data[:3]:  # 显示前3条
    #         print(person)
    
    print("数据处理器初始化完成")