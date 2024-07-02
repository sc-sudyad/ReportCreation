# main.py
from fastapi import FastAPI,Request
from fastapi.middleware.cors import CORSMiddleware

from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from routes.record_type_routes import record_router
from routes.no_of_records_routes import no_of_records
from routes.processing_time_routes import common_processing_router

app = FastAPI()
# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Include the record router
app.include_router(record_router, prefix="/api")

# Include the number of records router
app.include_router(no_of_records, prefix="/api2")

# Include the mean_processing router
app.include_router(common_processing_router, prefix="/api3")

templates = Jinja2Templates(directory="./templates")


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "name": "Your Name Here"})

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000)
