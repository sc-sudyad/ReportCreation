from typing import Optional

from fastapi import FastAPI, Request, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from services.record_type_service import RecordTypeService
from services.no_of_records_service import RecordsService
from services.processing_time_service import ProcessingTimeService
from repo.kafka_util import KafkaUtil
from repo.druid_util import DruidUtil
from exception_handling.druid_exception import DruidUtilError
from exception_handling.invalid_date_format import InvalidDateFormatError

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize templates
templates = Jinja2Templates(directory="./templates")

# Initialize utilities and services
druid_util = DruidUtil()
kafka_util = KafkaUtil()
kafka_service = RecordTypeService(kafka_util=kafka_util, druid_util=druid_util)
records_service = RecordsService(druid_util=druid_util)
processing_service = ProcessingTimeService(druid_util=druid_util)


# Kafka-related endpoints
@app.get("/api/get_processing_stats", status_code=status.HTTP_200_OK)
async def get_processing_stats(device_type: str, device_id: Optional[str] = None):
    try:
        return kafka_service.get_processing_stats(device_type, device_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/get_summary", status_code=status.HTTP_200_OK)
async def get_summary(device_type: str):
    try:
        return kafka_service.get_summary(device_type)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Records-related endpoints
@app.get("/api2/get_records_count_date_range", status_code=status.HTTP_200_OK)
async def get_records_count_date_range(device_type: str, start_date: str, end_date: str,
                                       device_id: Optional[str] = None):
    try:
        count = records_service.get_records_count_date_range(device_type, device_id, start_date, end_date)
        return [{"count": count}]
    except InvalidDateFormatError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except DruidUtilError as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from Druid. {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error {e}")


@app.get("/api2/get_records_count_aggregated", status_code=status.HTTP_200_OK)
async def get_records_count_aggregated(device_type: str, start_date: str, end_date: str, aggregation_type: str,
                                       device_id: Optional[str] = None):
    try:
        return records_service.get_records_count_aggregated(device_type, device_id, start_date, end_date,
                                                            aggregation_type)
    except InvalidDateFormatError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except DruidUtilError as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from Druid. {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error {e}")


# Processing time-related endpoints
@app.get("/api3/get_processing_time_date_range", status_code=status.HTTP_200_OK)
async def get_processing_time_date_range(device_type: str, start_date: str, end_date: str, metric_type: str,
                                         device_id: Optional[str] = None):
    try:
        return processing_service.get_processing_time_data_range(device_type, device_id, start_date, end_date,
                                                                 metric_type)
    except InvalidDateFormatError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except DruidUtilError as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from Druid. {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error {e}")


@app.get("/api3/get_processing_time_aggregated", status_code=status.HTTP_200_OK)
async def get_processing_time_aggregated(device_type: str, start_date: str, end_date: str, aggregation_type: str,
                                         metric_type: str, device_id: Optional[str] = None):
    try:
        return processing_service.get_processing_time_aggregated(device_type, device_id, start_date, end_date,
                                                                 aggregation_type, metric_type)
    except InvalidDateFormatError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except DruidUtilError as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from Druid. {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error {e}")


# Define root endpoint
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "name": "Your Name Here"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000)
