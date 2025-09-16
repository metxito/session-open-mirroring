from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Register the router from routes/core.py
from routes.core import register_routes
register_routes(app)

