#!/usr/bin/env python3
"""
预警数据合并脚本
将预警查询列表中的预警次数合并到取现记录中
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from data_process import DataProcessor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    """主函数"""
    try:
        # 设置文件路径
        base_dir = Path(__file__).parent.resolve()
        uploads_dir = base_dir / "uploads"
        
        # 预警查询列表文件
        warning_file = uploads_dir / "预警查询列表导出.xlsx"
        
        # 取现记录文件
        withdraw_file = uploads_dir / "取现记录导出.xlsx"
        
        # 检查文件是否存在
        if not warning_file.exists():
            print(f"错误: 找不到预警查询列表文件: {warning_file}")
            print("请确保文件位于 uploads 目录下，文件名为: 预警查询列表导出.xlsx")
            return 1
        
        if not withdraw_file.exists():
            print(f"错误: 找不到取现记录文件: {withdraw_file}")
            print("请确保文件位于 uploads 目录下，文件名为: 取现记录导出.xlsx")
            return 1
        
        # 创建输出文件名（带时间戳）
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_file = uploads_dir / f"合并结果_{timestamp}.xlsx"
        
        print("=" * 60)
        print("预警数据合并工具")
        print("=" * 60)
        print(f"预警查询列表: {warning_file}")
        print(f"取现记录文件: {withdraw_file}")
        print(f"输出文件: {output_file}")
        print("=" * 60)
        
        # 创建数据处理器
        processor = DataProcessor()
        
        # 执行合并
        print("\n开始处理...")
        merged_df = processor.merge_warning_count_to_withdraw_records(
            str(warning_file),
            str(withdraw_file),
            str(output_file)
        )
        
        # 显示结果统计
        print("\n" + "=" * 60)
        print("处理完成！")
        print("=" * 60)
        print(f"总记录数: {len(merged_df)}")
        print(f"有预警记录: {(merged_df['预警次数'] > 0).sum()}")
        print(f"无预警记录: {(merged_df['预警次数'] == 0).sum()}")
        print(f"最大预警次数: {merged_df['预警次数'].max()}")
        print(f"平均预警次数: {merged_df['预警次数'].mean():.2f}")
        print(f"\n输出文件: {output_file}")
        print("=" * 60)
        
        # 显示部分数据预览
        print("\n数据预览（前5行）:")
        print(merged_df.head())
        
        print("\n预警次数统计:")
        print(merged_df['预警次数'].value_counts().sort_index())
        
        return 0
        
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

