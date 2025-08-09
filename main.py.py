
# -*- coding: utf-8 -*-
import os, io, math, logging, json
from datetime import datetime
from typing import List, Dict

import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, CallbackQueryHandler, filters

# PDF export
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle

import threading, asyncio
from flask import Flask

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger("koolbot")

TELEGRAM_TOKEN = os.getenv("TOKEN") or os.getenv("TELEGRAM_TOKEN") or ""
UNIT_PRICE = float(os.getenv("UNIT_PRICE", "700"))
BASE_DIR = os.path.dirname(__file__)
EXCEL_FILE = os.path.join(BASE_DIR, "KOOLEXIL.xlsx")
LOGS_FILE  = os.path.join(BASE_DIR, "logs.csv")
ADMINS_FILE= os.path.join(BASE_DIR, "admins.json")

# ===== Admin helpers =====
def ensure_admins_exists():
    if not os.path.exists(ADMINS_FILE) or os.path.getsize(ADMINS_FILE)==0:
        data = {"users":[{"username":"مدير","pin":"1234","per_field":{}}]}
        with open(ADMINS_FILE, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)

def load_admins():
    ensure_admins_exists()
    with open(ADMINS_FILE, "r", encoding="utf-8") as f:
        try: return json.load(f)
        except: return {"users":[{"username":"مدير","pin":"1234","per_field":{}}]}

def save_admins(data):
    with open(ADMINS_FILE, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)

def get_admin_names():
    return [u.get("username","") for u in load_admins().get("users",[]) if u.get("username")]

def get_field_mode_for_user(username: str, field: str) -> str:
    """Return one of: read / edit / hide"""
    for u in load_admins().get("users",[]):
        if u.get("username")==username:
            pf = u.get("per_field") or {}
            mode = pf.get(field)
            if mode: return mode
            return "edit" if field in {"القراءة الحالية","المسدد","المتأخرات","رقم الهاتف","اسم المشترك"} else "read"
    return "edit" if field in {"القراءة الحالية","المسدد","المتأخرات","رقم الهاتف","اسم المشترك"} else "read"

# ===== Columns =====
BASE_COLS = ["اسم المشترك","رقم الهاتف","رقم العداد","القراءة السابقة","القراءة الحالية","الاستهلاك","قيمة الاستهلاك","المتأخرات","الإجمالي","المسدد","المتبقي"]
DISPLAY_ORDER = ["اسم المشترك","رقم الهاتف","رقم العداد","القراءة السابقة","القراءة الحالية","مستهلك/وحده","مستهلك/ريال","المتأخرات","الإجمالي","المسدد","المتبقي"]
EDITABLE_FIELDS = {"القراءة الحالية","المسدد","المتأخرات","رقم الهاتف","اسم المشترك"}

# ===== Arabic normalization =====
AR_DIAC="ًٌٍَُِّْ"; AR_DIAC_TABLE = str.maketrans("", "", AR_DIAC + "ـ"); AR_MAP=str.maketrans({"أ":"ا","إ":"ا","آ":"ا","ى":"ي","ة":"ه"})
def ar_norm(s): 
    if s is None: return ""
    s=str(s).strip().translate(AR_DIAC_TABLE).translate(AR_MAP).replace("\u200f","").replace("\u200e","")
    return " ".join(s.split()).lower()

ALIAS={"اسم المشترك":["اسم","المشترك","إسم المشترك","اسم  المشترك","اسم_المشترك"],"رقم الهاتف":["الهاتف","التلفون","رقم التلفون","رقم الجوال","الجوال","الموبايل","هاتف","تلفون"],"رقم العداد":["العداد","رقم  العداد","رقم-العداد"],"القراءة السابقة":["القراءه السابقه","قراءة سابقه","سابقه","السابقه"],"القراءة الحالية":["القراءه الحاليه","قراءة حاليه","الحاليه","حاليه"],"الاستهلاك":["مستهلك/وحده","مستهلك وحده","استهلاك","إستهلاك"],"قيمة الاستهلاك":["مستهلك/ريال","مستهلك ريال","قيمه الاستهلاك","قيمة-الاستهلاك"],"المتأخرات":["متاخرات","المتاخرات"],"الإجمالي":["الاجمالي","الاجمالي عليه","الإجمالي عليه","المجموع"],"المسدد":["المدفوع","مدفوع","المسدّد"],"المتبقي":["الباقي","الباقي عليه","المتبقي عليه"]}
CANON={ar_norm(k):k for k in ALIAS}
for k,arr in ALIAS.items():
    for a in arr: CANON[ar_norm(a)]=k

# ===== Excel helpers =====
def ensure_excel_exists():
    if not os.path.exists(EXCEL_FILE):
        pd.DataFrame(columns=BASE_COLS).to_excel(EXCEL_FILE, index=False)

def map_headers(df):
    df=df.rename(columns={c: CANON.get(ar_norm(c), c) for c in df.columns})
    for c in BASE_COLS:
        if c not in df.columns:
            df[c] = "" if c in {"اسم المشترك","رقم الهاتف","رقم العداد"} else 0
    return df

def load_df():
    ensure_excel_exists()
    df = pd.read_excel(EXCEL_FILE); df = map_headers(df)
    for c in ["القراءة السابقة","القراءة الحالية","الاستهلاك","قيمة الاستهلاك","المتأخرات","الإجمالي","المسدد","المتبقي","مستهلك/وحده","مستهلك/ريال"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce").fillna(0)
    for c in ["اسم المشترك","رقم الهاتف","رقم العداد"]:
        if c in df.columns: df[c]=df[c].astype(str).fillna("").str.strip()
    if "مستهلك/وحده" in df and "الاستهلاك" in df: df.loc[df["الاستهلاك"].eq(0), "الاستهلاك"]=df["مستهلك/وحده"]
    if "مستهلك/ريال" in df and "قيمة الاستهلاك" in df: df.loc[df["قيمة الاستهلاك"].eq(0), "قيمة الاستهلاك"]=df["مستهلك/ريال"]
    if {"المتبقي","الإجمالي","المسدد"}.issubset(df.columns): df["المتبقي"]=pd.to_numeric(df["الإجمالي"],errors="coerce").fillna(0)-pd.to_numeric(df["المسدد"],errors="coerce").fillna(0)
    return df

def save_df(df): df.to_excel(EXCEL_FILE, index=False)

# ===== Formatting =====
def strip_trailing_dot_zero(s): 
    if s is None: return ""
    ss=str(s).strip()
    if ss.endswith(".0"):
        try: return str(int(float(ss)))
        except: return ss
    return ss
def fmt_int(v):
    try:
        f=float(v)
        if math.isnan(f) or math.isinf(f): return "0"
        return str(int(round(f)))
    except: return strip_trailing_dot_zero(v)
def digits_only(s): return "".join(ch for ch in str(s) if ch.isdigit())
def normalize_for_match(s): return ar_norm(s).replace(" ","")

# ===== Computation =====
def recompute_row(row):
    current=float(row.get("القراءة الحالية",0) or 0); prev=float(row.get("القراءة السابقة",0) or 0)
    cons=max(current-prev,0); cons_val=cons*UNIT_PRICE
    arrears=float(row.get("المتأخرات",0) or 0); paid=float(row.get("المسدد",0) or 0)
    total=arrears+cons_val; remain=total-paid
    row["الاستهلاك"]=int(round(cons)); row["قيمة الاستهلاك"]=int(round(cons_val))
    row["الإجمالي"]=int(round(total)); row["المتبقي"]=int(round(remain))
    if "مستهلك/وحده" in row.index: row["مستهلك/وحده"]=row["الاستهلاك"]
    if "مستهلك/ريال" in row.index: row["مستهلك/ريال"]=row["قيمة الاستهلاك"]
    return row

# ===== Search =====
def find_row_indices(df, field, query):
    if field not in df.columns: return []
    q_raw=str(query).strip(); q_norm=normalize_for_match(q_raw); q_digits=digits_only(q_raw)
    hits=[]
    for i,v in df[field].fillna("").items():
        v_str=str(v); v_norm=normalize_for_match(v_str); v_digits=digits_only(v_str)
        if q_norm and q_norm in v_norm: hits.append(i)
        elif q_digits and q_digits in v_digits: hits.append(i)
        elif strip_trailing_dot_zero(v_str)==strip_trailing_dot_zero(q_raw): hits.append(i)
    return list(dict.fromkeys(hits))

# ===== UI: Keyboard =====
MAIN_KB = ReplyKeyboardMarkup([
    [KeyboardButton("➕ إضافة قراءة حالية"), KeyboardButton("💵 تسديد مبلغ")],
    [KeyboardButton("🔍 بحث برقم العداد"), KeyboardButton("🔎 بحث بالاسم")],
    [KeyboardButton("📞 بحث بالهاتف"), KeyboardButton("📤 تصدير البيانات")],
    [KeyboardButton("➕ إضافة مشترك"), KeyboardButton("👥 المسؤولين")],
], resize_keyboard=True)

# ===== Modes =====
MODE_NONE="none"; MODE_ADD_READING="add_reading"; MODE_SEARCH_METER="search_meter"; MODE_SEARCH_NAME="search_name"; MODE_SEARCH_PHONE="search_phone"; MODE_AWAIT_VALUE="await_value"; MODE_SEARCH_PAY="search_pay"
MODE_REPORT_DAY="report_day"; MODE_REPORT_RANGE="report_range"; MODE_REPORT_WAIT_START="report_wait_start"; MODE_REPORT_WAIT_END="report_wait_end"; MODE_REPORT_CHOOSE_FMT="report_choose_fmt"
MODE_ADD_SUB_NAME="add_sub_name"; MODE_ADD_SUB_PHONE="add_sub_phone"; MODE_ADD_SUB_METER="add_sub_meter"; MODE_ADD_SUB_PREV="add_sub_prev"; MODE_ADD_SUB_CURR="add_sub_curr"; MODE_ADD_SUB_ARREARS="add_sub_arrears"; MODE_ADD_SUB_PAID="add_sub_paid"
# Admin flow
MODE_ADMIN_NEW_NAME="admin_new_name"; MODE_ADMIN_NEW_PIN="admin_new_pin"

# ===== Activity Logging =====
def log_event(user_name, action, amount=0.0, meter="", subscriber=""):
    ts=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    header="timestamp,user,action,amount,meter,subscriber\n"
    line=f"{ts},{user_name},{action},{amount},{meter},{subscriber}\n"
    newfile = not os.path.exists(LOGS_FILE) or os.path.getsize(LOGS_FILE)==0
    with open(LOGS_FILE, "a", encoding="utf-8") as f:
        if newfile: f.write(header)
        f.write(line)

# ===== UI helpers =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("mode", MODE_NONE); context.user_data.setdefault("selected_index", None)
    # active admin used for field perms display; default to "مدير"
    context.user_data.setdefault("active_admin", "مدير")
    await update.message.reply_text("مرحبًا بك في لوحة التحكم 👇", reply_markup=MAIN_KB)

def format_vertical(row):
    renames={"رقم الهاتف":"الهاتف","الإجمالي":"الإجمالي عليه","المتبقي":"المتبقي عليه","قيمة الاستهلاك":"المستهلك/ريال","الاستهلاك":"المستهلك/وحده"}
    out=[]
    for k in DISPLAY_ORDER:
        if k in row.index: val=row.get(k,"")
        elif k=="مستهلك/وحده": val=row.get("الاستهلاك","")
        elif k=="مستهلك/ريال": val=row.get("قيمة الاستهلاك","")
        else: val=""
        if k in {"القراءة السابقة","القراءة الحالية","الاستهلاك","قيمة الاستهلاك","المتأخرات","الإجمالي","المسدد","المتبقي","مستهلك/وحده","مستهلك/ريال"}:
            v=fmt_int(val)
        elif k in {"رقم العداد","رقم الهاتف"}:
            v=strip_trailing_dot_zero(val)
        else:
            v="" if str(val).lower() in {"nan","none"} else str(val)
        title=renames.get(k,k); out.append(f"{title}\t{v}")
    return "\n".join(out)

def fields_inline_kb(cols: List[str], active_admin: str = None):
    rows=[]
    for col in cols:
        mode = "edit"
        try:
            if active_admin:
                mode = get_field_mode_for_user(active_admin, col)
        except Exception:
            mode = "edit"
        if mode == "hide":
            continue
        label = "✏️ "+col if (mode=="edit" and col in EDITABLE_FIELDS) else "👁️ "+col
        rows.append([InlineKeyboardButton(label, callback_data=f"field::{col}")])
    rows.append([InlineKeyboardButton("🔙 رجوع", callback_data="back_menu")])
    return InlineKeyboardMarkup(rows)

# ===== Text router =====
async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text=(update.message.text or "").strip()
    mode=context.user_data.get("mode", MODE_NONE)

    if text=="➕ إضافة قراءة حالية":
        context.user_data["mode"]=MODE_ADD_READING; context.user_data["add_field"]=None
        kb=InlineKeyboardMarkup([[InlineKeyboardButton("🔍 برقم العداد", callback_data="addread:meter")],[InlineKeyboardButton("🔎 بالاسم", callback_data="addread:name")],[InlineKeyboardButton("📞 بالهاتف", callback_data="addread:phone")],[InlineKeyboardButton("إلغاء", callback_data="addread:cancel")]])
        return await update.message.reply_text("اختر طريقة البحث لإضافة قراءة حالية:", reply_markup=kb)

    if text=="💵 تسديد مبلغ":
        context.user_data["mode"]=MODE_SEARCH_PAY; context.user_data["pay_field"]=None
        kb=InlineKeyboardMarkup([[InlineKeyboardButton("🔍 برقم العداد", callback_data="pay:meter")],[InlineKeyboardButton("🔎 بالاسم", callback_data="pay:name")],[InlineKeyboardButton("📞 بالهاتف", callback_data="pay:phone")],[InlineKeyboardButton("إلغاء", callback_data="pay:cancel")]])
        return await update.message.reply_text("اختر طريقة البحث لتسديد مبلغ:", reply_markup=kb)

    if text=="🔍 بحث برقم العداد":
        context.user_data["mode"]=MODE_SEARCH_METER
        return await update.message.reply_text("أدخل رقم العداد:", reply_markup=MAIN_KB)
    if text=="🔎 بحث بالاسم":
        context.user_data["mode"]=MODE_SEARCH_NAME
        return await update.message.reply_text("أدخل اسم المشترك:", reply_markup=MAIN_KB)
    if text=="📞 بحث بالهاتف":
        context.user_data["mode"]=MODE_SEARCH_PHONE
        return await update.message.reply_text("أدخل رقم الهاتف:", reply_markup=MAIN_KB)

    if text=="ايقونة الحقول":
        idx=context.user_data.get("selected_index")
        if idx is None: return await update.message.reply_text("⚠️ اختر مشتركًا أولًا بالبحث.", reply_markup=MAIN_KB)
        df=load_df()
        if idx not in df.index: return await update.message.reply_text("⚠️ السجل غير موجود.", reply_markup=MAIN_KB)
        return await update.message.reply_text("قائمة الحقول:", reply_markup=fields_inline_kb(list(df.columns), active_admin=context.user_data.get("active_admin","مدير")))

    if text=="📤 تصدير البيانات":
        kb=InlineKeyboardMarkup([[InlineKeyboardButton("📄 PDF", callback_data="export:pdf"), InlineKeyboardButton("📊 Excel", callback_data="export:excel")],[InlineKeyboardButton("إلغاء", callback_data="export:cancel")]])
        return await update.message.reply_text("اختر نوع الملف للتصدير:", reply_markup=kb)

    if text=="➕ إضافة مشترك":
        kb=InlineKeyboardMarkup([
            [InlineKeyboardButton("🆕 مشترك جديد", callback_data="sub:new")],
            [InlineKeyboardButton("🛠️ تعديل بيانات مشترك", callback_data="sub:edit")],
            [InlineKeyboardButton("إلغاء", callback_data="sub:cancel")],
        ])
        return await update.message.reply_text("اختر العملية:", reply_markup=kb)

    if text=="👥 المسؤولين":
        kb=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ مسؤول جديد", callback_data="admin:add")],
            [InlineKeyboardButton("🛠️ تعديل صلاحيات مسؤول", callback_data="admin:edit")],
            [InlineKeyboardButton("🗑️ حذف مسؤول", callback_data="admin:del")],
            [InlineKeyboardButton("📅 تقرير مجدول", callback_data="admin:schedule")],
            [InlineKeyboardButton("إلغاء", callback_data="admin:cancel")],
        ])
        return await update.message.reply_text("قائمة المسؤولين:", reply_markup=kb)

    # Admin add (name -> pin)
    if context.user_data.get("mode")==MODE_ADMIN_NEW_NAME:
        name=text.strip()
        if not name:
            return await update.message.reply_text("أدخل اسمًا صالحًا.", reply_markup=MAIN_KB)
        context.user_data["new_admin_name"]=name
        context.user_data["mode"]=MODE_ADMIN_NEW_PIN
        return await update.message.reply_text("أدخل رمز الدخول (PIN):", reply_markup=MAIN_KB)

    if context.user_data.get("mode")==MODE_ADMIN_NEW_PIN:
        pin=text.strip()
        if not pin:
            return await update.message.reply_text("أدخل PIN صالح.", reply_markup=MAIN_KB)
        name=context.user_data.get("new_admin_name")
        data=load_admins()
        if any(u.get("username")==name for u in data.get("users",[])):
            context.user_data["mode"]=MODE_NONE
            return await update.message.reply_text("⚠️ هذا الاسم موجود مسبقًا.", reply_markup=MAIN_KB)
        data.setdefault("users",[]).append({"username":name,"pin":pin,"per_field":{}})
        save_admins(data)
        context.user_data["mode"]=MODE_NONE
        return await update.message.reply_text(f"✅ تم إضافة المسؤول: {name}", reply_markup=MAIN_KB)

    # Awaiting value input
    if context.user_data.get("mode")==MODE_AWAIT_VALUE:
        return await handle_value_input(update, context)

    # Add subscriber flow
    if mode in {"add_sub_name","add_sub_phone","add_sub_meter","add_sub_prev","add_sub_curr","add_sub_arrears","add_sub_paid"}:
        return await handle_add_subscriber_flow(update, context, text)

    # Search flows
    if mode in (MODE_ADD_READING, MODE_SEARCH_METER, MODE_SEARCH_NAME, MODE_SEARCH_PHONE, MODE_SEARCH_PAY, "sub_edit_search"):
        return await handle_search(update, context, mode, text)

    return await update.message.reply_text("اختر من لوحة التحكم:", reply_markup=MAIN_KB)

# ===== Add subscriber flow =====
async def handle_add_subscriber_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    mode=context.user_data.get("mode"); new=context.user_data.get("new_sub", {})
    if mode=="add_sub_name":
        new["اسم المشترك"]=text; context.user_data["mode"]="add_sub_phone"
        return await update.message.reply_text("أدخل رقم الهاتف:", reply_markup=MAIN_KB)
    if mode=="add_sub_phone":
        new["رقم الهاتف"]=text; context.user_data["mode"]="add_sub_meter"
        return await update.message.reply_text("أدخل رقم العداد:", reply_markup=MAIN_KB)
    if mode=="add_sub_meter":
        new["رقم العداد"]=text; context.user_data["mode"]="add_sub_prev"
        return await update.message.reply_text("أدخل القراءة السابقة (رقم):", reply_markup=MAIN_KB)
    if mode=="add_sub_prev":
        try: new["القراءة السابقة"]=float(text)
        except: return await update.message.reply_text("⚠️ أدخل رقمًا صحيحًا للقراءة السابقة.", reply_markup=MAIN_KB)
        context.user_data["mode"]="add_sub_curr"
        return await update.message.reply_text("أدخل القراءة الحالية (رقم):", reply_markup=MAIN_KB)
    if mode=="add_sub_curr":
        try: new["القراءة الحالية"]=float(text)
        except: return await update.message.reply_text("⚠️ أدخل رقمًا صحيحًا للقراءة الحالية.", reply_markup=MAIN_KB)
        context.user_data["mode"]="add_sub_arrears"
        return await update.message.reply_text("أدخل المتأخرات (رقم):", reply_markup=MAIN_KB)
    if mode=="add_sub_arrears":
        try: new["المتأخرات"]=float(text)
        except: return await update.message.reply_text("⚠️ أدخل رقمًا صحيحًا للمتأخرات.", reply_markup=MAIN_KB)
        context.user_data["mode"]="add_sub_paid"
        return await update.message.reply_text("أدخل المسدد (رقم):", reply_markup=MAIN_KB)
    if mode=="add_sub_paid":
        try: new["المسدد"]=float(text)
        except: return await update.message.reply_text("⚠️ أدخل رقمًا صحيحًا للمسدد.", reply_markup=MAIN_KB)
        df=load_df()
        for c in BASE_COLS:
            if c not in new: new[c] = "" if c in {"اسم المشترك","رقم الهاتف","رقم العداد"} else 0
        row=pd.Series(new); row=recompute_row(row)
        df=pd.concat([df, pd.DataFrame([row])], ignore_index=True); save_df(df)
        context.user_data["mode"]=MODE_NONE; context.user_data["selected_index"]=int(df.index[-1])
        return await update.message.reply_text("✅ تمت إضافة المشترك وحُسبت القيم.", reply_markup=MAIN_KB)

# ===== Helpers =====
def fmt_display_title(row):
    name=str(row.get("اسم المشترك","")) or "—"
    meter=strip_trailing_dot_zero(row.get("رقم العداد",""))
    phone=strip_trailing_dot_zero(row.get("رقم الهاتف",""))
    return f"{name} | عداد: {meter} | هاتف: {phone}"

async def show_record(update: Update, context: ContextTypes.DEFAULT_TYPE, row: pd.Series):
    df=load_df(); idx=context.user_data.get("selected_index")
    if idx is not None and idx in df.index:
        df.loc[idx]=recompute_row(df.loc[idx]); save_df(df); row=df.loc[idx]
    return await update.message.reply_text(format_vertical(row), reply_markup=MAIN_KB)

# ===== Callback router =====
    # ----- Add/Edit subscriber menu -----
    if data.startswith("sub:"):
        kind = data.split(":",1)[1]
        if kind == "cancel":
            await q.answer("تم الإلغاء");
            return await q.message.reply_text("تم الإلغاء.", reply_markup=MAIN_KB)

        if kind == "new":
            await q.answer("مشترك جديد")
            context.user_data["mode"]=MODE_ADD_SUB_NAME; context.user_data["new_sub"]={}
            return await q.message.reply_text("أدخل اسم المشترك:", reply_markup=MAIN_KB)

        if kind == "edit":
            await q.answer("تعديل بيانات مشترك")
            context.user_data["mode"]="sub_edit_choose"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 برقم العداد", callback_data="subedit:meter")],
                [InlineKeyboardButton("🔎 بالاسم", callback_data="subedit:name")],
                [InlineKeyboardButton("📞 بالهاتف", callback_data="subedit:phone")],
                [InlineKeyboardButton("إلغاء", callback_data="sub:cancel")],
            ])
            return await q.message.reply_text("اختر طريقة البحث لتعديل بيانات مشترك:", reply_markup=kb)


    if data.startswith("subedit:"):
        kind = data.split(":",1)[1]
        field_map={"meter":"رقم العداد","name":"اسم المشترك","phone":"رقم الهاتف"}
        pick_field = field_map.get(kind, "رقم العداد")
        context.user_data["mode"]="sub_edit_search"; context.user_data["subedit_field"]=pick_field
        await q.answer()
        return await q.message.reply_text(f"أدخل {pick_field}:", reply_markup=MAIN_KB)


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query
    if not q: return
    data=q.data or ""

    if data.startswith("addread:"):
        kind=data.split(":",1)[1]
        if kind=="cancel":
            await q.answer("تم الإلغاء"); await q.message.reply_text("أُلغي الإجراء.", reply_markup=MAIN_KB); return
        field_map={"meter":"رقم العداد","name":"اسم المشترك","phone":"رقم الهاتف"}
        context.user_data["add_field"]=field_map.get(kind); context.user_data["mode"]=MODE_ADD_READING
        await q.answer(); return await q.message.reply_text(f"أدخل {field_map.get(kind)}:", reply_markup=MAIN_KB)

    if data.startswith("pay:"):
        kind=data.split(":",1)[1]
        if kind=="cancel":
            await q.answer("تم الإلغاء"); await q.message.reply_text("أُلغي الإجراء.", reply_markup=MAIN_KB); return
        field_map={"meter":"رقم العداد","name":"اسم المشترك","phone":"رقم الهاتف"}
        context.user_data["pay_field"]=field_map.get(kind); context.user_data["mode"]=MODE_SEARCH_PAY
        await q.answer(); return await q.message.reply_text(f"أدخل {field_map.get(kind)}:", reply_markup=MAIN_KB)

    if data.startswith("pick::"):
        try: idx=int(data.split("::",1)[1])
        except: await q.answer(); return
        context.user_data["selected_index"]=idx; df=load_df(); await q.answer("تم الاختيار")
        # If we are in sub edit flow, show all fields to choose one to edit
        if context.user_data.get("mode")=="sub_edit_search":
            cols = list(df.columns)
            return await q.message.reply_text("اختر الحقل المراد تعديله:", reply_markup=fields_inline_kb(cols, active_admin=context.user_data.get("active_admin","مدير")))
        if context.user_data.get("mode")==MODE_ADD_READING:
            context.user_data["edit_field"]="القراءة الحالية"; context.user_data["mode"]=MODE_AWAIT_VALUE
            cur=df.at[idx,"القراءة الحالية"] if "القراءة الحالية" in df.columns else 0
            prev=df.at[idx,"القراءة السابقة"] if "القراءة السابقة" in df.columns else 0
            return await q.message.reply_text(f"أدخل القيمة الجديدة للقراءة الحالية\n(الحالية الآن: {fmt_int(cur)} — السابقة: {fmt_int(prev)})", reply_markup=MAIN_KB)
        if context.user_data.get("mode")==MODE_SEARCH_PAY:
            context.user_data["edit_field"]="المسدد"; context.user_data["mode"]=MODE_AWAIT_VALUE
            usage=fmt_int(df.at[idx,"قيمة الاستهلاك"] if "قيمة الاستهلاك" in df.columns else 0)
            arrears=fmt_int(df.at[idx,"المتأخرات"] if "المتأخرات" in df.columns else 0)
            total=fmt_int(df.at[idx,"الإجمالي"] if "الإجمالي" in df.columns else 0)
            msg=f"الاستهلاك (ريال): {usage}\nالمتأخرات: {arrears}\nالإجمالي: {total}\nأدخل المبلغ المسدد:"
            return await q.message.reply_text(msg, reply_markup=MAIN_KB)
        return await q.message.reply_text(format_vertical(df.loc[idx]), reply_markup=MAIN_KB)

    if data.startswith("field::"):
        col = data.split("::",1)[1]
        # Determine mode for active admin
        modev = "edit"
        try:
            modev = get_field_mode_for_user(context.user_data.get("active_admin","مدير"), col)
        except Exception:
            modev = "edit"
        if modev == "hide":
            await q.answer("هذا الحقل مخفي")
            return
        # If admin allows only read, just display current value
        if modev == "read":
            idx = context.user_data.get("selected_index")
            if idx is None:
                await q.answer(); return
            df = load_df(); val = df.at[idx, col] if col in df.columns else ""
            return await q.message.reply_text(f"{col}: {fmt_int(val) if str(val).isdigit() else str(val)}", reply_markup=MAIN_KB)
        # Else ask for new value
        context.user_data["edit_field"] = col
        context.user_data["mode"] = MODE_AWAIT_VALUE
        idx = context.user_data.get("selected_index")
        df = load_df()
        cur = df.at[idx, col] if (idx is not None and col in df.columns) else ""
        return await q.message.reply_text(f"أدخل القيمة الجديدة لـ {col}\n(القيمة الحالية: {fmt_int(cur) if str(cur).isdigit() else str(cur)})", reply_markup=MAIN_KB)

    if data=="back_menu":
        await q.answer(); return await q.message.reply_text("العودة للوحة التحكم", reply_markup=MAIN_KB)

    if data=="export:excel":
        await q.answer("جارِ التحضير…"); await send_excel(update, context)
        user=(update.effective_user.username or update.effective_user.full_name or "guest"); log_event(user, "export_excel"); return
    if data=="export:pdf":
        await q.answer("جارِ التحضير…"); await send_pdf(update, context)
        user=(update.effective_user.username or update.effective_user.full_name or "guest"); log_event(user, "export_pdf"); return
    if data=="export:cancel":
        await q.answer("تم الإلغاء"); return await q.message.reply_text("تم إلغاء التصدير.", reply_markup=MAIN_KB)

    # Admin menu
    if data.startswith("admin:"):
        kind=data.split(":",1)[1]
        if kind=="cancel":
            await q.answer("تم الإلغاء"); return await q.message.reply_text("تم الإلغاء.", reply_markup=MAIN_KB)
        if kind=="add":
            await q.answer("إضافة مسؤول")
            context.user_data["mode"]=MODE_ADMIN_NEW_NAME
            return await q.message.reply_text("أدخل اسم المسؤول الجديد:", reply_markup=MAIN_KB)
        if kind=="edit":
            await q.answer("تعديل صلاحيات")
            names=get_admin_names()
            if not names:
                return await q.message.reply_text("لا يوجد مسؤولون بعد.", reply_markup=MAIN_KB)
            buttons=[[InlineKeyboardButton(n, callback_data=f"adminpick:{n}")] for n in names]
            buttons.append([InlineKeyboardButton("إلغاء", callback_data="admin:cancel")])
            return await q.message.reply_text("اختر المسؤول لتعديل صلاحياته:", reply_markup=InlineKeyboardMarkup(buttons))
        if kind=="del":
            await q.answer("حذف مسؤول")
            names=get_admin_names()
            buttons=[[InlineKeyboardButton(f"🗑️ {n}", callback_data=f"admindel:{n}")] for n in names]
            buttons.append([InlineKeyboardButton("إلغاء", callback_data="admin:cancel")])
            return await q.message.reply_text("اختر المسؤول لحذفه:", reply_markup=InlineKeyboardMarkup(buttons))
        if kind=="schedule":
            await q.answer()
            kb=InlineKeyboardMarkup([[InlineKeyboardButton("📅 يوم محدد", callback_data="report:day")],[InlineKeyboardButton("📆 بين تاريخين", callback_data="report:range")],[InlineKeyboardButton("📜 كامل السجل", callback_data="report:all")],[InlineKeyboardButton("إلغاء", callback_data="report:cancel")]])
            return await q.message.reply_text("اختر نوع المدة للتقرير:", reply_markup=kb)

    if data.startswith("adminpick:"):
        username=data.split(":",1)[1]
        context.user_data["admin_edit_target"]=username
        # Build matrix of fields with three options
        cols = BASE_COLS[:]
        rows=[]
        for c in cols:
            rows.append([InlineKeyboardButton(c, callback_data="noop")])
            rows.append([
                InlineKeyboardButton("👁️ قراءة", callback_data=f"perms:{username}:{c}:read"),
                InlineKeyboardButton("✏️ تحرير", callback_data=f"perms:{username}:{c}:edit"),
                InlineKeyboardButton("🙈 إخفاء", callback_data=f"perms:{username}:{c}:hide"),
            ])
        rows.append([InlineKeyboardButton("🔙 رجوع", callback_data="admin:edit")])
        return await q.message.reply_text(f"تعديل صلاحيات: {username}", reply_markup=InlineKeyboardMarkup(rows))

    if data.startswith("perms:"):
        _, username, field, modev = data.split(":",3)
        data_json = load_admins()
        found=False
        for u in data_json.get("users",[]):
            if u.get("username")==username:
                pf = u.get("per_field") or {}
                pf[field] = modev if modev in {"read","edit","hide"} else "read"
                u["per_field"] = pf
                found=True
                break
        if not found:
            data_json.setdefault("users",[]).append({"username":username,"pin":"1234","per_field":{field:modev}})
        save_admins(data_json)
        await q.answer("تم الحفظ")
        return

    if data.startswith("admindel:"):
        username=data.split(":",1)[1]
        data_json=load_admins()
        data_json["users"]=[u for u in data_json.get("users",[]) if u.get("username")!=username]
        save_admins(data_json)
        await q.answer("تم الحذف")
        return await q.message.reply_text(f"🗑️ تم حذف: {username}", reply_markup=MAIN_KB)

    # Reports
    if data.startswith("report:"):
        kind=data.split(":",1)[1]
        if kind=="cancel":
            await q.answer("تم الإلغاء"); return await q.message.reply_text("تم إلغاء التقرير.", reply_markup=MAIN_KB)
        if kind=="day":
            context.user_data["mode"]=MODE_REPORT_DAY; await q.answer()
            return await q.message.reply_text("أدخل التاريخ (YYYY-MM-DD):", reply_markup=MAIN_KB)
        if kind=="range":
            context.user_data["mode"]=MODE_REPORT_WAIT_START; await q.answer()
            return await q.message.reply_text("أدخل تاريخ البداية (YYYY-MM-DD):", reply_markup=MAIN_KB)
        if kind=="all":
            context.user_data["report_filter"]={"type":"all"}; context.user_data["mode"]=MODE_REPORT_CHOOSE_FMT; await q.answer()
            kb=InlineKeyboardMarkup([[InlineKeyboardButton("📄 PDF", callback_data="reportfmt:pdf"), InlineKeyboardButton("📊 Excel", callback_data="reportfmt:excel")]])
            return await q.message.reply_text("اختر صيغة التقرير:", reply_markup=kb)

    if data.startswith("reportfmt:"):
        fmt=data.split(":",1)[1]; await q.answer("جارِ إنشاء التقرير…")
        await generate_and_send_report(update, context, fmt); context.user_data["mode"]=MODE_NONE; return

# ===== Search handler =====
async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str, text: str):
    df=load_df()
    if mode==MODE_ADD_READING: field=context.user_data.get("add_field") or "رقم العداد"
    elif mode==MODE_SEARCH_PAY: field=context.user_data.get("pay_field") or "رقم العداد"
    elif mode==MODE_SEARCH_METER: field="رقم العداد"
    elif mode==MODE_SEARCH_NAME: field="اسم المشترك"
    elif mode=="sub_edit_search": field=context.user_data.get("subedit_field") or "رقم العداد"
    else: field="رقم الهاتف"
    idxs=find_row_indices(df, field, text)
    if not idxs: return await update.message.reply_text("❌ لا توجد نتائج مطابقة.", reply_markup=MAIN_KB)
    if len(idxs)>1:
        buttons=[[InlineKeyboardButton(f"اختيار: {fmt_display_title(df.loc[i])}", callback_data=f"pick::{i}")] for i in idxs]
        kb=InlineKeyboardMarkup(buttons+[[InlineKeyboardButton("إلغاء", callback_data="pick:cancel")]])
        return await update.message.reply_text("اختر السجل المطلوب:", reply_markup=kb)
    i=idxs[0]; context.user_data["selected_index"]=int(i)
    if mode==MODE_ADD_READING:
        context.user_data["edit_field"]="القراءة الحالية"; context.user_data["mode"]=MODE_AWAIT_VALUE
        cur=df.at[i,"القراءة الحالية"] if "القراءة الحالية" in df.columns else 0
        prev=df.at[i,"القراءة السابقة"] if "القراءة السابقة" in df.columns else 0
        return await update.message.reply_text(f"أدخل القيمة الجديدة للقراءة الحالية\n(الحالية الآن: {fmt_int(cur)} — السابقة: {fmt_int(prev)})", reply_markup=MAIN_KB)
    if mode==MODE_SEARCH_PAY:
        context.user_data["edit_field"]="المسدد"; context.user_data["mode"]=MODE_AWAIT_VALUE
        usage=fmt_int(df.at[i,"قيمة الاستهلاك"] if "قيمة الاستهلاك" in df.columns else 0)
        arrears=fmt_int(df.at[i,"المتأخرات"] if "المتأخرات" in df.columns else 0)
        total=fmt_int(df.at[i,"الإجمالي"] if "الإجمالي" in df.columns else 0)
        return await update.message.reply_text(f"الاستهلاك (ريال): {usage}\nالمتأخرات: {arrears}\nالإجمالي: {total}\nأدخل المبلغ المسدد:", reply_markup=MAIN_KB)
    return await show_record(update, context, df.loc[i])

# ===== Edit value (with special rules) =====
async def handle_value_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    idx=context.user_data.get("selected_index"); col=context.user_data.get("edit_field")
    if idx is None or not col:
        context.user_data["mode"]=MODE_NONE; return await update.message.reply_text("لا يوجد سياق تعديل نشط.", reply_markup=MAIN_KB)
    df=load_df()
    if idx not in df.index:
        context.user_data["mode"]=MODE_NONE; return await update.message.reply_text("السجل غير موجود.", reply_markup=MAIN_KB)
    val=update.message.text.strip()
    if col=="القراءة الحالية":
        try: new_curr=float(val)
        except: return await update.message.reply_text("⚠️ أدخل رقمًا صحيحًا.", reply_markup=MAIN_KB)
        old_curr=float(df.at[idx,"القراءة الحالية"]) if "القراءة الحالية" in df.columns else 0
        old_remain=float(df.at[idx,"المتبقي"]) if "المتبقي" in df.columns else 0
        # 1) السابقة = الحالية القديمة
        if "القراءة السابقة" in df.columns: df.at[idx,"القراءة السابقة"]=old_curr
        # 2) المتأخرات = المتبقي القديم (استبدال)
        if "المتأخرات" in df.columns: df.at[idx,"المتأخرات"]=old_remain
        # 3) المسدد = 0
        if "المسدد" in df.columns: df.at[idx,"المسدد"]=0
        # تحديث الحالية
        df.at[idx,"القراءة الحالية"]=new_curr
        # سجل العملية
        user=(update.effective_user.username or update.effective_user.full_name or "guest")
        row=df.loc[idx]; log_event(user, "update_reading", amount=0, meter=str(row.get("رقم العداد","")), subscriber=str(row.get("اسم المشترك","")))
    elif col=="المسدد":
        try: val_num=float(val)
        except: return await update.message.reply_text("⚠️ أدخل رقمًا صحيحًا.", reply_markup=MAIN_KB)
        df.at[idx,"المسدد"]=val_num
        user=(update.effective_user.username or update.effective_user.full_name or "guest")
        row=df.loc[idx]; log_event(user, "pay", amount=val_num, meter=str(row.get("رقم العداد","")), subscriber=str(row.get("اسم المشترك","")))
    elif col in EDITABLE_FIELDS - {"اسم المشترك","رقم الهاتف"}:
        try: val_num=float(val)
        except: return await update.message.reply_text("⚠️ أدخل رقمًا صحيحًا.", reply_markup=MAIN_KB)
        df.at[idx, col]=val_num
    else:
        df.at[idx, col]=val
    df.loc[idx]=recompute_row(df.loc[idx]); save_df(df)
    context.user_data["mode"]=MODE_NONE
    return await update.message.reply_text("✅ تم التحديث.", reply_markup=MAIN_KB)

# ===== Export helpers =====
def df_clean_for_export(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in BASE_COLS if c in df.columns] + [c for c in df.columns if c not in BASE_COLS]
    df = df[cols].copy()
    for c in df.columns:
        num = pd.to_numeric(df[c], errors="coerce")
        if num.notna().any():
            nums = num.fillna(0).astype(float).round(0).astype(int).astype(str)
            nums[num.isna()] = ""
            df[c] = nums
        else:
            df[c] = df[c].astype(str).replace({"nan":"", "None":""}).fillna("")
    return df

def calc_col_widths_for_page(num_cols: int, page_width: float, left_margin: float=12, right_margin: float=12):
    usable = page_width - left_margin - right_margin
    if num_cols <= 0: return []
    w = usable / num_cols
    return [w] * num_cols

def df_to_pdf_landscape(df, out_path, title="تصدير البيانات"):
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle

import threading, asyncio
from flask import Flask
    from reportlab.lib.pagesizes import A4, landscape
    cdf = df_clean_for_export(df)
    data = [list(cdf.columns)] + cdf.astype(str).values.tolist()
    pagesize = landscape(A4)
    left=12; right=12; top=14; bottom=14
    doc = SimpleDocTemplate(out_path, pagesize=pagesize, rightMargin=right, leftMargin=left, topMargin=top, bottomMargin=bottom)
    col_widths = calc_col_widths_for_page(len(cdf.columns), pagesize[0], left, right)
    table = Table(data, colWidths=col_widths, repeatRows=1)
    style = TableStyle([('BACKGROUND',(0,0),(-1,0),colors.lightgrey),('GRID',(0,0),(-1,-1),0.25,colors.grey),('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,-1),8),('ALIGN',(0,0),(-1,0),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE')])
    # right-align numeric-ish columns
    for ci,col in enumerate(cdf.columns):
        try:
            col_vals = cdf[col].astype(str).str.replace(r'[^0-9]', '', regex=True)
            ratio = (col_vals.str.len() > 0).sum() / max(1, len(col_vals))
            if ratio > 0.6:
                style.add('ALIGN', (ci,1), (ci,-1), 'RIGHT')
        except: pass
    table.setStyle(style)
    doc.build([table])

async def send_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    df=load_df(); df=df_clean_for_export(df); bio=io.BytesIO(); df.to_excel(bio, index=False); bio.seek(0)
    await update.effective_chat.send_document(document=InputFile(bio, filename="KOOLEXIL.xlsx"), caption="📦 ملف Excel الحالي")
    await update.effective_chat.send_message("العودة للوحة التحكم:", reply_markup=MAIN_KB)

async def send_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    df=load_df(); df=df_clean_for_export(df); tmp=os.path.join(BASE_DIR,"export.pdf"); df_to_pdf_landscape(df, tmp, title="تصدير البيانات")
    with open(tmp,"rb") as f: pdf_bytes=f.read()
    bio=io.BytesIO(pdf_bytes); bio.seek(0)
    await update.effective_chat.send_document(document=InputFile(bio, filename="KOOLEXIL.pdf"), caption="📄 PDF (أفقي)")
    try: os.remove(tmp)
    except: pass
    await update.effective_chat.send_message("العودة للوحة التحكم:", reply_markup=MAIN_KB)

# ===== Reports =====
async def generate_and_send_report(update: Update, context: ContextTypes.DEFAULT_TYPE, fmt="excel"):
    if not os.path.exists(LOGS_FILE) or os.path.getsize(LOGS_FILE)==0:
        return await update.effective_chat.send_message("لا يوجد سجل عمليات بعد.", reply_markup=MAIN_KB)
    df=pd.read_csv(LOGS_FILE)
    try: df["date"]=pd.to_datetime(df["timestamp"]).dt.date
    except: pass
    filt=context.user_data.get("report_filter", {"type":"all"})
    if filt.get("type")=="day":
        try: target=pd.to_datetime(filt.get("day")).date(); df=df[df["date"]==target]
        except: return await update.effective_chat.send_message("صيغة التاريخ غير صحيحة لليوم المحدد.", reply_markup=MAIN_KB)
    elif filt.get("type")=="range":
        try: start=pd.to_datetime(filt.get("start")).date(); end=pd.to_datetime(filt.get("end")).date(); df=df[(df["date"]>=start)&(df["date"]<=end)]
        except: return await update.effective_chat.send_message("صيغة التاريخ غير صحيحة لنطاق التواريخ.", reply_markup=MAIN_KB)
    df["amount"]=pd.to_numeric(df.get("amount",0), errors="coerce").fillna(0)
    summary=df.groupby("user").agg(عدد_العمليات=("action","count"), اجمالي_المسددة=("amount","sum")).reset_index().rename(columns={"user":"المسؤول"})
    if summary.empty: return await update.effective_chat.send_message("لا توجد بيانات ضمن المدة المحددة.", reply_markup=MAIN_KB)
    if fmt=="pdf":
        tmp=os.path.join(BASE_DIR,"report.pdf"); df_to_pdf_landscape(summary, tmp, title="تقرير مجدول")
        with open(tmp,"rb") as f: pdf_bytes=f.read()
        bio=io.BytesIO(pdf_bytes); bio.seek(0)
        await update.effective_chat.send_document(InputFile(bio, filename="scheduled_report.pdf"), caption="تقرير مجدول (PDF)")
        try: os.remove(tmp)
        except: pass
    else:
        bio=io.BytesIO(); summary.to_excel(bio, index=False); bio.seek(0)
        await update.effective_chat.send_document(InputFile(bio, filename="scheduled_report.xlsx"), caption="تقرير مجدول (Excel)")
    await update.effective_chat.send_message("العودة للوحة التحكم:", reply_markup=MAIN_KB)

async def text_date_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text=(update.message.text or "").strip()
    mode=context.user_data.get("mode")
    if mode=="report_day":
        context.user_data["report_filter"]={"type":"day","day":text}
        context.user_data["mode"]=MODE_REPORT_CHOOSE_FMT
        kb=InlineKeyboardMarkup([[InlineKeyboardButton("📄 PDF", callback_data="reportfmt:pdf"), InlineKeyboardButton("📊 Excel", callback_data="reportfmt:excel")]])
        return await update.message.reply_text("اختر صيغة التقرير:", reply_markup=kb)
    if mode=="report_wait_start":
        context.user_data["report_filter"]={"type":"range","start":text}
        context.user_data["mode"]="report_wait_end"
        return await update.message.reply_text("أدخل تاريخ النهاية (YYYY-MM-DD):", reply_markup=MAIN_KB)
    if mode=="report_wait_end":
        filt=context.user_data.get("report_filter",{"type":"range"}); filt["end"]=text; context.user_data["report_filter"]=filt
        context.user_data["mode"]=MODE_REPORT_CHOOSE_FMT
        kb=InlineKeyboardMarkup([[InlineKeyboardButton("📄 PDF", callback_data="reportfmt:pdf"), InlineKeyboardButton("📊 Excel", callback_data="reportfmt:excel")]])
        return await update.message.reply_text("اختر صيغة التقرير:", reply_markup=kb)
    # otherwise forward to general router
    return await text_router(update, context)

def build_app():
    app=ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_date_router))
    return app


if __name__=="__main__":
    # Ensure local files exist
    ensure_excel_exists(); ensure_admins_exists()
    log.info("✅ بدء تشغيل البوت مع خادم إبقاء حيّة (Flask) — يعمل على Render Free")
    
    async def run_bot():
        tg_app = build_app()
        # run_polling هو كوروتين في PTB v20+
        await tg_app.run_polling(drop_pending_updates=True, allowed_updates=["message","callback_query"])
    
    def bot_thread():
        asyncio.run(run_bot())
    
    # شغّل البوت في خيط منفصل
    threading.Thread(target=bot_thread, daemon=True).start()
    
    # خادم بسيط لإبقاء الخدمة مستيقظة
    web = Flask(__name__)
    
    @web.get("/")
    def home():
        return "OK - Nader Water Bot"
    
    port = int(os.environ.get("PORT", "10000"))
    # يعمل على 0.0.0.0 حتى يكون متاحًا من الإنترنت
    web.run(host="0.0.0.0", port=port)
