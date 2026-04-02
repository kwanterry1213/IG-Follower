# Staff Promotion Leaderboard (Streamlit)

## Run

```powershell
pip install -r requirements.txt
python -m streamlit run app.py
```

## Usage

- Public leaderboard: open `http://localhost:8501/`
- Scan link (staff): `http://localhost:8501/?sid=<staff_id>`

The app logs valid scans to `scan_log.csv` locally and then redirects to the configured Instagram URL.

