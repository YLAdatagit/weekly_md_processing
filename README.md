# Weekly MD Processing Pipeline
(အပတ်စဉ် MasterDB ဒေတာများကို လုပ်ဆောင်သောစနစ်)

ဤပရောဂျက်သည် "MasterDB" အပတ်စဉ် ဒေတာများကို ဖြည်ချခြင်း (extraction)၊ ပြောင်းလဲခြင်း (transformation) နှင့် ဒေတာဘေ့စ်သို့ ထည့်သွင်းခြင်း (loading) လုပ်ငန်းစဉ်များကို အလိုအလျောက် လုပ်ဆောင်ပေးပါသည်။

## Features (လုပ်ဆောင်နိုင်စွမ်းများ)

- WinRAR ကိုအသုံးပြု၍ RAR archive များထဲမှ အပတ်စဉ် Excel ဖိုင်များကို ဖြည်ချပေးခြင်း။
- LTE နှင့် NR ပိုင်းဆိုင်ရာ worksheet များကို သန့်စင်ပြီး CSV ဖော်မတ်သို့ ပြောင်းလဲပေးခြင်း။
- သန့်စင်ပြီးသော CSV data များကို PostgreSQL database သို့ အလိုအလျောက် ထည့်သွင်းပေးခြင်း။
- Site data များအတွက် ဆက်စပ်မှုရှိသော (flattened) GeoJSON ဖိုင်တစ်ခုကို ဖန်တီးပေးခြင်း။

## Requirements (လိုအပ်ချက်များ)

- Python 3.9 (သို့မဟုတ်) ထို့ထက်အသစ်
- WinRAR (ပုံမှန်အားဖြင့် `C:\Program Files\WinRAR\WinRAR.exe` တွင် ရှိရမည်)
- PostgreSQL Database
- လိုအပ်သော Python packages များ -
  - pandas
  - geopandas
  - shapely
  - psycopg2
  - python-dotenv
  - rarfile

## Setup (ထည့်သွင်းပြင်ဆင်ခြင်း)

၁။ လိုအပ်သော Python package များကို install လုပ်ပါ -
   ```bash
   pip install -r requirements.txt
   ```
   (သို့မဟုတ် `pip install pandas geopandas shapely psycopg2 python-dotenv rarfile` ဟုလည်း အသုံးပြုနိုင်ပါသည်။)

၂။ Project root folder ထဲတွင် `.env` အမည်ဖြင့် ဖိုင်တစ်ခု ဖန်တီးပြီး အောက်ပါ အချက်အလက်များကို ထည့်သွင်းပါ -
   ```env
   DB_NAME=postgres
   DB_USER=postgres
   DB_PASSWORD=your_password
   DB_HOST=localhost
   DB_PORT=5432

   BASE_DIR="D:/path/to/data"
   OUTPUT_DIR="D:/path/to/output"

   region=BMA
   WEEK_NUM=WK2531
   ```
   *(`BASE_DIR` နှင့် `OUTPUT_DIR` လမ်းကြောင်းများ၊ Database အချက်အလက်များကို မိမိစက်နှင့်အညီ ပြောင်းလဲထည့်သွင်းပါ။ `WEEK_NUM` နှင့် `region` ကို script run သည့်အခါ အလိုအလျောက် ပြောင်းလဲပေးမည်ဖြစ်သည်။)*

၃။ WinRAR ကို စက်ထဲတွင် Install လုပ်ထားပြီး မှန်ကန်သော လမ်းကြောင်းတွင် ရှိနေကြောင်း သေချာစေပါ။

## Usage (အသုံးပြုပုံ)

သတ်မှတ်ထားသော အပတ်စဉ် (week) နှင့် ဒေသ (region) တစ်ခုအတွက် လုပ်ငန်းစဉ်တစ်ခုလုံးကို အောက်ပါ command ဖြင့် စတင်အသုံးပြုနိုင်ပါသည် -

```bash
python set_date.py WK2531 BMA
```
(ဥပမာ - `NEA` region အတွက်ဆိုလျှင် `python set_date.py WK2531 NEA` ဟု အသုံးပြုနိုင်ပါသည်။)

ယင်း script သည် `.env` ဖိုင်ထဲရှိ `WEEK_NUM` နှင့် `region` တန်ဖိုးများကို အလိုအလျောက် အပ်ဒိတ်လုပ်ပေးမည်ဖြစ်ပြီး ဖြည်ချခြင်း၊ ဒေတာဘေ့စ်သို့ ထည့်သွင်းခြင်း လုပ်ငန်းစဉ်များကို ဆက်လက်လုပ်ဆောင်သွားမည်ဖြစ်သည်။

## Outputs (ရလဒ်များ)

- လုပ်ဆောင်ပြီးသော CSV ဖိုင်များနှင့် GeoJSON ဖိုင်များကို `.env` ဖိုင်တွင် သတ်မှတ်ထားသော `OUTPUT_DIR` ဖိုင်တွဲအောက်တွင် တွေ့ရှိနိုင်ပါသည်။
- လုပ်ငန်းစဉ် မှတ်တမ်းများ (Logs) များကို project folder ထဲရှိ `sync_log.log` ဖိုင်တွင် သိမ်းဆည်းထားမည်ဖြစ်သည်။
