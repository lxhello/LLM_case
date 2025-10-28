import os
from datetime import datetime
from pathlib import Path
from typing import Tuple
import json

from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, jsonify
from werkzeug.utils import secure_filename

# 导入评分系统和数据处理器
from scoring_system import WithdrawScoringSystem
from data_process import DataProcessor


BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {"csv", "xlsx", "xls"}


def create_app() -> Flask:
	app = Flask(__name__)
	app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")
	app.config["UPLOAD_FOLDER"] = str(UPLOAD_DIR)
	app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024  # 32MB

	# 初始化评分系统和数据处理器
	scoring_system = WithdrawScoringSystem()
	data_processor = DataProcessor()

	@app.route("/", methods=["GET"])
	def index():
		return render_template("index.html")

	@app.route("/upload", methods=["POST"])
	def upload():
		files = request.files.getlist("file")  # 获取所有上传的文件
		datetime_str = request.form.get("datetime") or ""
		amount_str = request.form.get("amount") or ""

		# 检查是否有文件上传
		if not files or not any(file.filename for file in files):
			flash("请选择至少一个文件", "error")
			return redirect(url_for("index"))

		# 验证第一个文件（用于验证时间和金额）
		file = files[0] if files else None
		ok, msg = _validate_inputs(file, datetime_str, amount_str)
		if not ok:
			flash(msg, "error")
			return redirect(url_for("index"))

		# Parse values
		when = _parse_datetime(datetime_str)
		amount = float(amount_str)

		# 处理所有上传的文件
		saved_files = []
		for file in files:
			if file and hasattr(file, 'filename') and file.filename:
				original_filename = file.filename
				filename = secure_filename(original_filename)
				
				# 检查文件名是否有效
				if not filename or filename == '':
					flash(f"文件名无效: {original_filename}，已跳过", "error")
					continue
				
				# 检查文件是否有扩展名
				if '.' not in filename:
					# 尝试从原始文件名中获取扩展名
					if '.' in original_filename:
						ext = original_filename.rsplit('.', 1)[1].lower()
						if ext in ['csv', 'xlsx', 'xls']:
							filename = f"{filename}.{ext}"
						else:
							flash(f"不支持的文件格式: .{ext}，已跳过 {original_filename}", "error")
							continue
					else:
						flash(f"文件缺少扩展名: {original_filename}，已跳过", "error")
						continue
				
				app.logger.info(f"原始文件名: {original_filename}, 处理后文件名: {filename}")
				save_path = UPLOAD_DIR / filename
				file.save(save_path)
				saved_files.append(filename)
				
				# 对每个文件执行评分分析
				if when:
					try:
						# 使用数据处理器处理上传文件
						processed_data = data_processor.process_uploaded_data(str(save_path))
						if processed_data and data_processor.validate_processed_data(processed_data):
							# 执行评分分析
							scoring_result = scoring_system.perform_analysis(when, amount, "", str(save_path), processed_data)
							
							# 保存评分结果
							result_path = UPLOAD_DIR / f"{filename}_scoring_result.json"
							scoring_system.export_analysis_result(scoring_result, str(result_path))
					except Exception as e:
						app.logger.warning(f"文件处理失败: {str(e)}，仅保存文件")

		# Log and flash message
		if saved_files:
			files_str = ", ".join(saved_files[:3])  # 只显示前3个文件名
			if len(saved_files) > 3:
				files_str += f" 等{len(saved_files)}个文件"
			else:
				files_str = files_str
			
			if when:
				app.logger.info("Received %d files: %s, when=%s, amount=%.2f", len(saved_files), files_str, when.isoformat(), amount)
				flash(f"已接收: {len(saved_files)} 个文件，时间 {when.strftime('%Y-%m-%d %H:%M')}，金额 {amount:.2f}，已完成评分分析", "success")
			else:
				app.logger.info("Received %d files: %s, amount=%.2f", len(saved_files), files_str, amount)
				flash(f"已接收: {len(saved_files)} 个文件，金额 {amount:.2f}", "success")
		else:
			flash("没有成功上传任何文件", "error")
		
		return redirect(url_for("index"))

	@app.route("/uploads/<path:filename>")
	def uploaded_file(filename: str):
		return send_from_directory(app.config["UPLOAD_FOLDER"], filename)

	@app.route("/logo.png")
	def logo():
		return send_from_directory(os.path.join(app.root_path, 'templates'), 'logo.png')

	@app.route("/scoring", methods=["POST"])
	def scoring():
		"""执行评分分析的API接口"""
		payload = request.get_json(silent=True) or {}
		clue_time = payload.get("clue_time")
		clue_amount = payload.get("clue_amount")
		clue_location = payload.get("clue_location", "")
		
		if not clue_time or not clue_amount:
			return jsonify({"success": False, "error": "缺少线索时间或金额"}), 400
		
		# 解析线索时间
		target_time = _parse_datetime(clue_time)
		if not target_time:
			return jsonify({"success": False, "error": "时间格式不正确"}), 400
		
		try:
			target_amount = float(clue_amount)
		except ValueError:
			return jsonify({"success": False, "error": "金额格式不正确"}), 400
		
		try:
			# 查找最新上传的文件
			latest_file = _get_latest_uploaded_file()
			
			if not latest_file:
				return jsonify({"success": False, "error": "未找到上传的数据文件，请先上传Excel或CSV文件"}), 400
			
			try:
				# 使用数据处理器处理上传文件
				app.logger.info(f"正在处理上传文件: {latest_file}")
				processed_data = data_processor.process_uploaded_data(str(latest_file))
				
				if not processed_data:
					return jsonify({"success": False, "error": "上传的文件中没有有效的数据"}), 400
				
				if not data_processor.validate_processed_data(processed_data):
					return jsonify({"success": False, "error": "数据验证失败，文件格式不正确"}), 400
				
				# 使用处理后的数据进行评分
				scoring_result = scoring_system.score_persons(processed_data, target_time, target_amount, clue_location)
				scoring_result["data_source"] = {
					"type": "uploaded_file",
					"file_name": latest_file.name,
					"file_path": str(latest_file),
					"processed_count": len(processed_data)
				}
				app.logger.info(f"成功处理上传文件，共{len(processed_data)}条数据")
				
			except Exception as process_error:
				# 文件处理失败
				app.logger.error(f"文件处理失败: {str(process_error)}")
				return jsonify({"success": False, "error": f"文件处理失败: {str(process_error)}"}), 500
			
			app.logger.info(f"评分分析完成: 线索时间={target_time}, 线索金额={target_amount}, 数据来源={scoring_result['data_source']['type']}")
			
			# 保存评分结果到文件以供后续 AI 分析使用
			result_path = UPLOAD_DIR / f"{latest_file.name}_scoring_result.json"
			try:
				with open(result_path, 'w', encoding='utf-8') as f:
					json.dump(scoring_result, f, ensure_ascii=False, indent=2, default=str)
				app.logger.info(f"评分结果已保存到: {result_path}")
			except Exception as e:
				app.logger.warning(f"保存评分结果失败: {str(e)}")
			
			return jsonify({"success": True, "data": scoring_result})
			
		except Exception as e:
			app.logger.error(f"评分分析失败: {str(e)}")
			return jsonify({"success": False, "error": f"评分分析失败: {str(e)}"}), 500

	return app


def _allowed_file(filename: str) -> bool:
	"""检查文件扩展名是否允许"""
	if not filename or '.' not in filename:
		return False
	ext = filename.rsplit('.', 1)[1].lower()
	return ext in ALLOWED_EXTENSIONS

def _check_file_security(file) -> Tuple[bool, str]:
	"""检查文件安全性"""
	if not file or not hasattr(file, 'filename') or not file.filename:
		return False, "无效文件"
	
	original_filename = file.filename
	app.logger.info(f"检查文件安全性: {original_filename}")
	
	# 检查文件是否有扩展名
	if '.' not in original_filename:
		return False, f"文件缺少扩展名: {original_filename}"
	
	# 检查文件类型
	if not _allowed_file(original_filename):
		ext = original_filename.rsplit('.', 1)[1].lower()
		return False, f"不允许的文件类型: .{ext}，支持的格式: {", ".join([f'.{ext}' for ext in ALLOWED_EXTENSIONS])}"
	
	# 检查文件大小
	file.seek(0, 2)  # 移动到文件末尾
	file_size = file.tell()
	file.seek(0)  # 重置文件指针
	
	max_size = 10 * 1024 * 1024  # 10MB
	if file_size > max_size:
		return False, f"文件太大: {file_size // 1024 // 1024}MB > 10MB"
	
	return True, "OK"


def _validate_inputs(file, datetime_str: str, amount_str: str) -> Tuple[bool, str]:
	# 使用安全检查
	file_ok, file_msg = _check_file_security(file)
	if not file_ok:
		return False, file_msg
	
	if not datetime_str:
		return False, "请选择日期时间"
	if _parse_datetime(datetime_str) is None:
		return False, "时间格式不正确"
	if not amount_str:
		return False, "请输入金额"
	try:
		amount = float(amount_str)
		if amount <= 0:
			return False, "金额必须大于0"
	except ValueError:
		return False, "金额必须为数字"
	return True, "OK"


def _parse_datetime(value: str):
	try:
		# HTML datetime format: 'YYYY-MM-DDTHH:MM'
		return datetime.strptime(value, "%Y-%m-%dT%H:%M")
	except Exception:
		return None


def _fix_file_extension_if_needed(file_path):
	"""修复文件扩展名问题"""
	try:
		file_obj = Path(file_path)
		if not file_obj.exists():
			return None
		
		# 如果文件没有扩展名，尝试检测文件类型
		if not file_obj.suffix:
			try:
				# 尝试读取文件头部来检测类型
				with open(file_path, 'rb') as f:
					header = f.read(8)
					
					# Excel 文件的魔术数字
					if header.startswith(b'PK'):
						# XLSX 文件
						new_path = file_obj.with_suffix('.xlsx')
						file_obj.rename(new_path)
						return new_path
					elif header.startswith(b'\xd0\xcf\x11\xe0'):
						# XLS 文件
						new_path = file_obj.with_suffix('.xls')
						file_obj.rename(new_path)
						return new_path
					else:
						# 尝试作为 CSV 文件
						new_path = file_obj.with_suffix('.csv')
						file_obj.rename(new_path)
						return new_path
			except Exception as e:
				print(f"文件类型检测失败: {e}")
				return file_obj
		else:
			return file_obj
			
	except Exception as e:
		print(f"修复文件扩展名失败: {e}")
		return file_path

def _get_latest_uploaded_file():
	"""获取最新上传的文件"""
	try:
		# 获取uploads目录下的所有文件
		files = list(UPLOAD_DIR.glob('*'))
		# 过滤掉结果文件
		all_files = [
			f for f in files 
			if f.is_file() 
			and not f.name.endswith('_scoring_result.json')
		]
		
		data_files = []
		for f in all_files:
			# 先检查有扩展名的文件
			if f.suffix.lower() in ['.csv', '.xlsx', '.xls']:
				data_files.append(f)
			else:
				# 尝试修复无扩展名的文件
				fixed_file = _fix_file_extension_if_needed(f)
				if fixed_file and fixed_file != f and fixed_file.suffix.lower() in ['.csv', '.xlsx', '.xls']:
					data_files.append(fixed_file)
		
		if data_files:
			# 按修改时间排序，返回最新的
			latest_file = max(data_files, key=lambda f: f.stat().st_mtime)
			return latest_file
		return None
	except Exception as e:
		print(f"获取最新文件失败: {str(e)}")
		return None


app = create_app()


if __name__ == "__main__":
	app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)


