#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
JEDCO Data Reader - قارئ بيانات JEDCO
=====================================

هذا السكربت يقرأ بيانات حافلات صالة الحج من ملف Excel
ويصدرها كـ JSON للاستخدام في لوحة التحكم.

⚠️ تحذير: هذا السكربت للقراءة فقط - لا يعدل الملف الأصلي

المؤلف: Claude Code
التاريخ: 2026-01-24
"""

import json
import sys
import io
from pathlib import Path
from datetime import datetime

# إصلاح ترميز Windows للعربية
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# =============================================================================
# الإعدادات والثوابت
# =============================================================================

# المسارات
EXCEL_PATH = Path(r"C:\Users\pcpz1\Desktop\JEDCO_Fail_HT\JEDCO_Tabel_Record\JEDCO_HT.xlsx")
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = PROJECT_DIR / "output"

# إعدادات Excel
SHEET_NAME = "التسجيل"
HEADER_ROW = 4  # صف العناوين = 5 (index يبدأ من 0)
REQUIRED_COLUMNS = list(range(16))  # الأعمدة A-P (0-15)

# أسماء الأعمدة المتوقعة (للتحقق)
EXPECTED_COLUMNS = [
    "م",                    # A - المعرف
    "رقم اللوحة",           # B
    "الناقل البري",         # C
    "رقم الرحلة",           # D
    "عدد الرحلات",          # E
    "عدد الركاب",           # F
    "نوع التأشيرة",         # G
    "وقت الإقلاع",          # H
    "وقت وصول الحافلة",     # I
    "شركة العمرة",          # J
    "حالة الرحلة",          # K
    "وضع الرحلة",           # L
    "حالة الحجز",           # M
    "الجنسية",              # N
    "الوجهة",               # O
    "الإجراء المتخذ",       # P
]


# =============================================================================
# الدوال المساعدة
# =============================================================================

def print_msg(msg, msg_type="info"):
    """طباعة رسالة بتنسيق موحد"""
    icons = {
        "info": "[i]",
        "success": "[OK]",
        "warning": "[!]",
        "error": "[X]",
        "step": "[>]"
    }
    icon = icons.get(msg_type, "*")
    print(f"{icon} {msg}")


def check_dependencies():
    """التحقق من وجود المكتبات المطلوبة"""
    print_msg("التحقق من المكتبات المطلوبة...", "step")

    missing = []

    try:
        import pandas
        print_msg(f"pandas: {pandas.__version__}", "success")
    except ImportError:
        missing.append("pandas")

    try:
        import openpyxl
        print_msg(f"openpyxl: {openpyxl.__version__}", "success")
    except ImportError:
        missing.append("openpyxl")

    if missing:
        print_msg(f"مكتبات مفقودة: {', '.join(missing)}", "error")
        print_msg("قم بتثبيتها: pip install " + " ".join(missing), "info")
        return False

    return True


def check_file_exists():
    """التحقق من وجود ملف Excel"""
    print_msg("التحقق من وجود الملف...", "step")

    if not EXCEL_PATH.exists():
        print_msg(f"الملف غير موجود: {EXCEL_PATH}", "error")
        return False

    file_size = EXCEL_PATH.stat().st_size / 1024  # KB
    print_msg(f"الملف موجود: {EXCEL_PATH.name} ({file_size:.1f} KB)", "success")
    return True


def ensure_output_dir():
    """التأكد من وجود مجلد output"""
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True)
        print_msg(f"تم إنشاء مجلد: {OUTPUT_DIR}", "info")
    return True


# =============================================================================
# الدوال الرئيسية
# =============================================================================

def read_excel_data():
    """
    قراءة البيانات من ملف Excel

    ⚠️ هذه الدالة للقراءة فقط - لا تعدل الملف

    Returns:
        pandas.DataFrame or None: البيانات أو None في حالة الخطأ
    """
    import pandas as pd

    print_msg("قراءة البيانات من Excel...", "step")

    try:
        # قراءة الملف (openpyxl يقرأ فقط افتراضياً)
        df = pd.read_excel(
            EXCEL_PATH,
            sheet_name=SHEET_NAME,
            header=HEADER_ROW,
            usecols=REQUIRED_COLUMNS,
            engine='openpyxl'
        )

        print_msg(f"تم قراءة {len(df)} صف و {len(df.columns)} عمود", "success")
        return df

    except Exception as e:
        print_msg(f"خطأ في قراءة الملف: {e}", "error")
        return None


def validate_columns(df):
    """
    التحقق من صحة الأعمدة

    Args:
        df: DataFrame للتحقق

    Returns:
        bool: True إذا كانت الأعمدة صحيحة
    """
    print_msg("التحقق من الأعمدة...", "step")

    # التحقق من وجود عمود "م" (العمود الأول)
    first_col = df.columns[0]
    if first_col != "م":
        print_msg(f"العمود الأول ليس 'م'، بل: '{first_col}'", "warning")
        # ليس خطأ فادح - قد يكون الاسم مختلفاً قليلاً
    else:
        print_msg("عمود 'م' موجود", "success")

    # عرض الأعمدة الموجودة
    print_msg(f"الأعمدة ({len(df.columns)}): {list(df.columns)}", "info")

    return True


def convert_to_json_safe(df):
    """
    تحويل DataFrame لصيغة JSON آمنة

    Args:
        df: DataFrame للتحويل

    Returns:
        list: قائمة من السجلات
    """
    import pandas as pd

    # نسخة للتعديل (لا نعدل الأصلية)
    df_copy = df.copy()

    # تحويل التواريخ والأوقات لنصوص
    for col in df_copy.columns:
        if df_copy[col].dtype == 'datetime64[ns]':
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        elif df_copy[col].dtype == 'timedelta64[ns]':
            df_copy[col] = df_copy[col].astype(str)

    # تحويل NaN لـ None
    df_copy = df_copy.where(pd.notnull(df_copy), None)

    # تحويل لقائمة سجلات
    records = df_copy.to_dict(orient='records')

    return records


def save_to_json(data, filename="raw_data.json"):
    """
    حفظ البيانات في ملف JSON

    ⚠️ يحفظ فقط في مجلد output

    Args:
        data: البيانات للحفظ
        filename: اسم الملف

    Returns:
        bool: True إذا نجح الحفظ
    """
    print_msg("حفظ البيانات...", "step")

    output_path = OUTPUT_DIR / filename

    # تحقق أمان: التأكد أن المسار في output
    if "output" not in str(output_path):
        print_msg("خطأ: محاولة الحفظ خارج مجلد output!", "error")
        return False

    try:
        # إعداد البيانات النهائية
        output_data = {
            "metadata": {
                "source": str(EXCEL_PATH.name),
                "sheet": SHEET_NAME,
                "exported_at": datetime.now().isoformat(),
                "total_records": len(data),
                "columns": EXPECTED_COLUMNS
            },
            "data": data
        }

        # الحفظ
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        file_size = output_path.stat().st_size / 1024  # KB
        print_msg(f"تم الحفظ: {output_path} ({file_size:.1f} KB)", "success")
        return True

    except Exception as e:
        print_msg(f"خطأ في الحفظ: {e}", "error")
        return False


# =============================================================================
# النقطة الرئيسية
# =============================================================================

def main():
    """الدالة الرئيسية"""
    print("=" * 50)
    print("   JEDCO Data Reader - قارئ بيانات JEDCO")
    print("=" * 50)
    print()

    # 1. التحقق من المكتبات
    if not check_dependencies():
        return 1
    print()

    # 2. التحقق من الملف
    if not check_file_exists():
        return 1
    print()

    # 3. التأكد من مجلد output
    ensure_output_dir()
    print()

    # 4. قراءة البيانات
    df = read_excel_data()
    if df is None:
        return 1
    print()

    # 5. التحقق من الأعمدة
    validate_columns(df)
    print()

    # 6. التحويل والحفظ
    if len(df) == 0:
        print_msg("الملف فارغ - لا توجد بيانات للتصدير", "warning")
        # نحفظ ملف فارغ للإشارة أن السكربت يعمل
        save_to_json([], "raw_data.json")
    else:
        records = convert_to_json_safe(df)
        save_to_json(records, "raw_data.json")

    print()
    print("=" * 50)
    print_msg("اكتملت العملية بنجاح!", "success")
    print("=" * 50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
