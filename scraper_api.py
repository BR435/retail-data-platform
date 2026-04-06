from fastapi import FastAPI
import subprocess

app = FastAPI()

@app.get("/run-scraper")
def run_scraper():
    try:
        subprocess.run(["python", "-m", "scrapers.falcon_scraper"], check=True)
        return {"status": "success", "message": "Scraper completed"}
    except Exception as e:
        return {"status": "error", "message": str(e)}