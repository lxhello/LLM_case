#!/usr/bin/env python3
"""
展示预警数据统计结果
"""

import sys
import logging
from pathlib import Path
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
        
        # 输出文件（直接更新原文件）
        output_file = withdraw_file
        
        # 检查文件是否存在
        if not warning_file.exists():
            print(f"错误: 找不到预警查询列表文件: {warning_file}")
            return 1
        
        if not withdraw_file.exists():
            print(f"错误: 找不到取现记录文件: {withdraw_file}")
            return 1
        
        print("=" * 70)
        print("预警数据合并工具")
        print("=" * 70)
        print(f"预警查询列表: {warning_file.name}")
        print(f"取现记录文件: {withdraw_file.name}")
        print(f"将合并结果直接更新到: {withdraw_file.name}")
        print("=" * 70)
        
        # 创建数据处理器
        processor = DataProcessor()
        
        # 执行合并
        print("\n开始处理...")
        print("1. 统计预警查询列表中每个受害人号码的出现次数")
        print("2. 将统计结果匹配到取现记录中")
        print("3. 更新取现记录导出文件\n")
        
        merged_df = processor.merge_warning_count_to_withdraw_records(
            str(warning_file),
            str(withdraw_file),
            str(output_file)
        )
        
        # 显示结果统计
        print("\n" + "=" * 70)
        print("处理完成！")
        print("=" * 70)
        print(f"总记录数: {len(merged_df)}")
        print(f"有预警记录: {(merged_df['预警次数'] > 0).sum()}")
        print(f"无预警记录: {(merged_df['预警次数'] == 0).sum()}")
        print(f"最大预警次数: {merged_df['预警次数'].max()}")
        print(f"平均预警次数: {merged_df['预警次数'].mean():.2f}")
        
        # 显示预警次数分布
        print(f"\n预警次数分布:")
        counts = merged_df['预警次数'].value_counts().sort_index()
        for count, num in counts.items():
            print(f"  预警{int(count)}次: {num}人")
        
        print(f"\n输出文件已保存: {output_file}")
        print("=" * 70)
        
        # 显示有预警的记录（如果存在）
        warning_records = merged_df[merged_df['预警次数'] > 0]
        if len(warning_records) > 0:
            print(f"\n有预警记录的人员（前10条）:")
            # 只显示存在的列
            display_cols = []
            for col in ['身份证号', '姓名', '电话号码', '预警次数', '疑似诈骗类型']:
                if col in warning_records.columns:
                    display_cols.append(col)
            if display_cols:
                print(warning_records[display_cols].head(10).to_string(index=False))
        else:
            print("\n注意: 没有找到匹配的预警记录")
            print("原因可能是:")
            print("  1. 预警查询列表中的'受害人身份证号'与取现记录中的'身份证号'不匹配")
            print("  2. 数据格式不一致（如带空格、特殊字符等）")
            print("  3. 两个文件中的数据确实没有交集")
        
        return 0
        
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

