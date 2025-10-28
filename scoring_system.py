"""
取现信息评分系统模块
根据线索时间和金额对取现人员进行评分分析，找出最符合条件的目标人员
"""

import json
import logging

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union


class WithdrawScoringSystem:
    """取现信息评分系统"""
    
    def __init__(self):
        """
        初始化评分系统，设置评分规则
        """
        # 根据情报线索的金额设置分级
        self.amount_thresholds = {
            'low': 30000,       # 3万以下
            'medium': 100000    # 10万分界线
        }
        
        # 基础分权重配置
        self.base_score_config = {
            'time_weight': 40,      # 时间匹配 40分
            'amount_weight': 40,    # 金额匹配 40分
            'location_weight': 40   # 地址匹配 40分
        }
        
        # 附加分配置 (根据金额区间有所不同)
        self.additional_score_config = {
            'low_amount': {  # 3万以下
                'male': 15,           # 男性加分
                'adult_app': 20,      # 涉黄类APP加分
                'history_warning': 10, # 历史预警加分
            },
            'medium_amount': {  # 3万到10万
                'male': 5,
                'female': 5,
                'adult_app': 10,
                'history_warning': 15,
                'special_app': 10
            },
            'high_amount': {  # 10万以上
                'female': 15,         # 女性加分
                'special_comm': 20,   # 小众通联加分
                'investment_app': 15,    # 投资类APP加分
                'history_warning': 10,  # 历史预警加分
            }
        }
        
        # 地点评分映射 (最高 40 分)
        self.location_scores = {
            "定海": 40,
            "普陀": 40,
            "岱山": 40,
            "嵊泗": 40,
            "其他": 40
        }

        # 设置日志
        self.logger = logging.getLogger(__name__)
    
    def get_amount_category(self, amount: float) -> str:
        """
        根据金额获取类别
        
        Args:
            amount: 金额
        
        Returns:
            str: 金额类别 ('low', 'medium', 'high')
        """
        if amount < self.amount_thresholds['low']:
            return 'low'
        elif amount < self.amount_thresholds['medium']:
            return 'medium'
        else:
            return 'high'
    
    def calculate_base_score(self, person_data: Dict[str, Any], target_time: datetime, target_amount: float, target_location: str = "") -> Dict[str, float]:
        """
        计算基础分（时间 + 金额 + 地址）
        
        Args:
            person_data: 人员数据
            target_time: 目标时间
            target_amount: 目标金额
            target_location: 目标地点
        
        Returns:
            Dict[str, float]: 基础分明细
        """
        base_scores = {
            'time_score': 0.0,
            'amount_score': 0.0,
            'location_score': 0.0,
            'total_base_score': 0.0
        }
        
        # 1. 时间匹配分 (40分)
        person_time = person_data.get('withdraw_time')
        if isinstance(person_time, datetime):
            time_diff_hours = abs((target_time - person_time).total_seconds()) / 3600
            if time_diff_hours <= 1:
                base_scores['time_score'] = 40
            elif time_diff_hours <= 3:
                base_scores['time_score'] = 35
            elif time_diff_hours <= 6:
                base_scores['time_score'] = 30
            elif time_diff_hours <= 12:
                base_scores['time_score'] = 25
            elif time_diff_hours <= 24:
                base_scores['time_score'] = 20
            elif time_diff_hours <= 48:
                base_scores['time_score'] = 15
            elif time_diff_hours <= 72:
                base_scores['time_score'] = 10
            else:
                base_scores['time_score'] = 5
        
        # 2. 金额匹配分 (40分)
        person_amount = float(person_data.get('amount', 0))
        if person_amount > 0:
            amount_diff_ratio = abs(target_amount - person_amount) / max(target_amount, person_amount)
            if amount_diff_ratio <= 0.05:  # 5%以内
                base_scores['amount_score'] = 40
            elif amount_diff_ratio <= 0.1:  # 10%以内
                base_scores['amount_score'] = 35
            elif amount_diff_ratio <= 0.2:  # 20%以内
                base_scores['amount_score'] = 30
            elif amount_diff_ratio <= 0.3:  # 30%以内
                base_scores['amount_score'] = 25
            elif amount_diff_ratio <= 0.5:  # 50%以内
                base_scores['amount_score'] = 20
            elif amount_diff_ratio <= 0.7:  # 70%以内
                base_scores['amount_score'] = 15
            elif amount_diff_ratio <= 1.0:  # 100%以内
                base_scores['amount_score'] = 10
            else:
                base_scores['amount_score'] = 5
        
        # 3. 地址匹配分 (40分)
        person_location = person_data.get('location', '')
        if target_location and target_location.strip():
            # 检查人员地点是否包含目标地点信息
            target_keywords = target_location.strip().split('、')  # 支持多个地点，用中文逗号分隔
            location_matched = False
            
            for keyword in target_keywords:
                keyword = keyword.strip()
                if keyword and keyword in person_location:
                    location_matched = True
                    break
            
            if location_matched:
                base_scores['location_score'] = 40  # 匹配上给满分
            else:
                base_scores['location_score'] = 0   # 未匹配不给分
        else:
            # 如果没有提供目标地点，默认给满分
            base_scores['location_score'] = 40
        
        # 计算基础分总分
        base_scores['total_base_score'] = (
            base_scores['time_score'] + 
            base_scores['amount_score'] + 
            base_scores['location_score']
        )
        
        return base_scores
    
    def calculate_additional_score(self, person_data: Dict[str, Any], target_amount: float) -> Dict[str, float]:
        """
        计算附加分（根据金额区间和个人特征）
        
        Args:
            person_data: 人员数据
            target_amount: 目标金额
        
        Returns:
            Dict[str, float]: 附加分明细
        """
        additional_scores = {
            'gender_score': 0.0,
            'app_score': 0.0,
            'history_warning_score': 0.0,
            'frequency_score': 0.0,  # 交易频次加分
            'total_additional_score': 0.0
        }
        
        amount_category = self.get_amount_category(target_amount)
        config = self.additional_score_config[f'{amount_category}_amount']
        
        # 获取人员特征
        gender = person_data.get('gender', 'unknown')
        has_adult_app = person_data.get('has_adult_app', False)
        has_special_comm = person_data.get('has_special_comm', False)
        has_history_warning = person_data.get('has_history_warning', False)
        
        # 根据金额区间计算附加分
        if amount_category == 'low':  # 3万以下
            # 男性加分
            if gender == 'male':
                additional_scores['gender_score'] += config['male']
            
            # 涉黄类APP加分
            if has_adult_app:
                additional_scores['app_score'] += config['adult_app']
                
        elif amount_category == 'medium':  # 3万到10万
            # 性别加分（男女都有少量加分）
            if gender == 'male':
                additional_scores['gender_score'] += config['male']
            elif gender == 'female':
                additional_scores['gender_score'] += config['female']
            
            # 涉黄类APP加分
            if has_adult_app:
                additional_scores['app_score'] += config['adult_app']
            
            # 特殊APP加分
            if has_special_comm:
                additional_scores['app_score'] += config['special_app']
                
        else:  # 10万以上
            # 女性加分
            if gender == 'female':
                additional_scores['gender_score'] += config['female']
            
            # 小众通联加分
            if has_special_comm:
                additional_scores['app_score'] += config['special_comm']
            
            # 投资类APP加分
            if person_data.get('has_investment_app', False):
                additional_scores['app_score'] += config['investment_app']
        
        # 历史预警加分（所有区间通用）
        if has_history_warning:
            additional_scores['history_warning_score'] = config['history_warning']
        
        # 交易频次加分（所有区间通用）
        transaction_count = person_data.get('transaction_count', 1)  # 默认为1次
        if transaction_count >= 2:
            # 交易频次两次及以上的，每多一次+5分
            additional_scores['frequency_score'] = (transaction_count - 1) * 5
        
        # 计算附加分总分
        additional_scores['total_additional_score'] = (
            additional_scores['gender_score'] + 
            additional_scores['app_score'] + 
            additional_scores['history_warning_score'] +
            additional_scores['frequency_score']
        )
        
        return additional_scores
    
    def score_person_new(self, person_data: Dict[str, Any], target_time: datetime, target_amount: float, target_location: str = "") -> Dict[str, Any]:
        """
        分层评分系统
        
        Args:
            person_data: 人员数据
            target_time: 目标时间  
            target_amount: 目标金额
            target_location: 目标地点
        
        Returns:
            Dict[str, Any]: 评分结果
        """
        try:
            # 计算基础分
            base_scores = self.calculate_base_score(person_data, target_time, target_amount, target_location)
            
            # 计算附加分
            additional_scores = self.calculate_additional_score(person_data, target_amount)
            
            # 总分 = 基础分 + 附加分
            total_score = base_scores['total_base_score'] + additional_scores['total_additional_score']
            
            # 确定匹配等级
            if total_score >= 100:
                match_level = "非常高"
            elif total_score >= 80:
                match_level = "高"
            elif total_score >= 60:
                match_level = "中"
            elif total_score >= 40:
                match_level = "低"
            else:
                match_level = "很低"
            
            # 获取金额类别
            amount_category = self.get_amount_category(target_amount)
            
            return {
                'name': person_data.get('name', '未知'),
                'account': person_data.get('account', ''),
                'id_number': person_data.get('id_number', ''),  # 添加身份证号码字段
                'withdraw_time': person_data.get('withdraw_time'),
                'amount': person_data.get('amount', 0),
                'location': person_data.get('location', ''),
                'status': person_data.get('status', ''),
                'gender': person_data.get('gender', 'unknown'),
                'has_adult_app': person_data.get('has_adult_app', False),
                'has_special_comm': person_data.get('has_special_comm', False),
                'has_history_warning': person_data.get('has_history_warning', False),
                'has_investment_app': person_data.get('has_investment_app', False),
                'score': round(total_score, 1),
                'match_level': match_level,
                'amount_category': amount_category,
                'score_details': {
                    'base_scores': base_scores,
                    'additional_scores': additional_scores,
                    'total_base_score': base_scores['total_base_score'],
                    'total_additional_score': additional_scores['total_additional_score']
                }
            }
            
        except Exception as e:
            self.logger.error(f"评分计算失败: {str(e)}")
            return {
                'name': person_data.get('name', '未知'),
                'score': 0,
                'match_level': '错误',
                'error': str(e)
            }
    

    def generate_sample_data(self) -> List[Dict[str, Any]]:
        """警告：不再生成示例数据，请使用真实上传数据"""
        self.logger.warning("警告：系统不再支持示例数据，请上传真实数据文件")
        raise ValueError("系统不再支持示例数据，请上传真实的Excel或CSV数据文件")
    
    def score_persons(self, persons: List[Dict[str, Any]], target_time: datetime, 
                     target_amount: float, target_location: str = "") -> Dict[str, Any]:
        """根据分层规则对人员进行评分
        
        Args:
            persons: 人员数据列表
            target_time: 目标时间
            target_amount: 目标金额
            target_location: 目标地点
        
        Returns:
            Dict[str, Any]: 评分结果
        """
        if not persons:
            return {
                "total_persons": 0,
                "target_info": {
                    "time": target_time.strftime("%Y-%m-%d %H:%M"),
                    "amount": target_amount
                },
                "scored_persons": [],
                "top_match": None,
                "amount_category": self.get_amount_category(target_amount)
            }
        
        # 获取金额类别
        amount_category = self.get_amount_category(target_amount)
        self.logger.info(f"目标金额: {target_amount}, 分类: {amount_category}")
        
        scored_persons = []
        
        # 使用新的评分系统
        for person in persons:
            scored_person = self.score_person_new(person, target_time, target_amount, target_location)
            scored_persons.append(scored_person)
        
        # 按分数排序
        scored_persons.sort(key=lambda x: x["score"], reverse=True)
        
        return {
            "total_persons": len(scored_persons),
            "target_info": {
                "time": target_time.strftime("%Y-%m-%d %H:%M"),
                "amount": target_amount,
                "location": target_location
            },
            "scored_persons": scored_persons,
            "top_match": scored_persons[0] if scored_persons else None,
            "amount_category": amount_category,
            "scoring_method": "分层评分系统"
        }
    

    

    

    

    

    

    

    

    

    
    def perform_analysis(self, clue_time: datetime, clue_amount: float, clue_location: str = "",
                        file_path: Optional[str] = None, custom_data: Optional[List[Dict[str, Any]]] = None, 
                        include_risk_assessment: bool = True) -> Dict[str, Any]:
        """
        执行完整的评分分析，仅支持真实数据
        
        Args:
            clue_time: 线索时间
            clue_amount: 线索金额
            clue_location: 线索地点
            file_path: 文件路径（已废弃）
            custom_data: 从文件处理器获取的真实数据
            include_risk_assessment: 是否包含风险评估
        
        Returns:
            Dict[str, Any]: 完整的分析结果
        """
        try:
            # 检查是否有有效数据
            if not custom_data:
                raise ValueError("缺少数据：请上传有效的Excel或CSV数据文件")
            
            # 使用真实数据进行评分
            result = self.score_persons(custom_data, clue_time, clue_amount, clue_location)
            
            # 构建分析结果
            analysis_result = {
                "clue_info": {
                    "time": clue_time.strftime("%Y-%m-%d %H:%M"),
                    "amount": clue_amount,
                    "location": clue_location
                },
                "analysis_result": result,
                "data_source": {
                    "type": "uploaded_file",
                    "file_path": file_path if file_path else None,
                    "data_count": len(custom_data)
                },
                "scoring_rules": {
                    "scenario_type": result.get('scenario_type', 'default'),
                    "time_scoring": "基于时间差距的递减评分",
                    "amount_scoring": "基于金额差异比例的递减评分",
                    "location_scoring": self.location_scores
                }
            }
            
            # 添加文件信息
            if file_path:
                import os
                analysis_result["file_info"] = {
                    "name": os.path.basename(file_path),
                    "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            
            # 添加统计信息
            analysis_result["statistics"] = {
                "total_analyzed": len(custom_data)
            }
            
            self.logger.info(f"评分分析完成，数据来源: uploaded_file，共分析 {len(custom_data)} 个人员")
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"评分分析失败: {str(e)}")
            return {
                "error": f"评分分析失败: {str(e)}",
                "clue_info": {
                    "time": clue_time.strftime("%Y-%m-%d %H:%M") if clue_time else "未知",
                    "amount": clue_amount if clue_amount else 0,
                    "location": clue_location if clue_location else ""
                }
            }
    
    def export_analysis_result(self, analysis_result: Dict[str, Any], output_path: str):
        """导出分析结果到JSON文件"""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(analysis_result, f, ensure_ascii=False, indent=2, default=str)
            self.logger.info(f"分析结果已导出到: {output_path}")
        except Exception as e:
            self.logger.error(f"导出分析结果失败: {str(e)}")
            raise
    
    def get_top_matches(self, analysis_result: Dict[str, Any], top_n: int = 3) -> List[Dict[str, Any]]:
        """获取前N个最佳匹配"""
        if "analysis_result" not in analysis_result or "scored_persons" not in analysis_result["analysis_result"]:
            return []
        
        scored_persons = analysis_result["analysis_result"]["scored_persons"]
        return scored_persons[:top_n]
    
if __name__ == "__main__":
    # 测试评分系统 - 仅显示初始化信息
    scoring_system: WithdrawScoringSystem = WithdrawScoringSystem()