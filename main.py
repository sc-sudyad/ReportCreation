

import logging
from typing import Optional, List

from fastapi import FastAPI, Request, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from services.summary_service import SummaryService
from services.no_of_records_service import RecordsService
from services.processing_time_service import ProcessingTimeService

from exception_handling.exception import DruidUtilError, KafkaUtilError
from exception_handling.invalid_date_format import InvalidDateFormatError

from config.logging_config import logger

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

summary_service = SummaryService()
records_service = RecordsService()
processing_service = ProcessingTimeService()


# Kafka-related endpoints
@app.get("/get_processing_stats", status_code=status.HTTP_200_OK)
async def get_processing_stats(input_topic: str, output_topic: str, device_type: Optional[str] = None,
                               device_id: Optional[str] = None):
    logger.info(f"Received request for /get_processing_stats")
    try:
        result = await summary_service.get_processing_stats(input_topic, output_topic, device_type, device_id)
        return result
    except KafkaUtilError as e:
        logger.error(f"KafkaUtilError occurred: {e}")
        raise HTTPException(status_code=500, detail=f"KafkaUtilError occurred: {e}")
    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")


@app.get("/get_summary_all_datasource", status_code=status.HTTP_200_OK)
async def get_summary_all_datasource(datasource_query: List[str] = Query(...)):
    logger.info(f"Received request for /get_summary_all_datasource")
    try:
        response = await summary_service.get_summary_all_datasource(datasource_query)
        return response
    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_latest_summary_by_interval", status_code=status.HTTP_200_OK)
async def get_latest_summary_by_interval(datasource: List[str] = Query(...), interval_type: str = Query()):
    logger.info(f"Received request for /get_latest_summary_by_interval")
    try:
        response = await summary_service.get_latest_summary_by_interval(datasource, interval_type)
        return response
    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/get_device_id_per_datasource", status_code=status.HTTP_200_OK)
async def get_device_id_per_datasource(datasource: str):
    logger.info(f"Received request for /get_device_id_per_datasource")
    try:
        response = await summary_service.get_device_id_per_datasource(datasource)
        return response
    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Records-related endpoints

@app.get("/get_records_count_date_range", status_code=status.HTTP_200_OK)
async def get_records_count_date_range(datasource: str, start_date: str, end_date: str,
                                       device_id: Optional[str] = None):
    logger.info(f"Received request for /get_records_count_date_range")
    try:
        response = await records_service.get_records_count_date_range(datasource, device_id, start_date, end_date)
        return response
    except InvalidDateFormatError as e:
        logger.error(f"Invalid date format: {e}")
        raise HTTPException(
            status_code=400, detail=f"Invalid date format: {e}")
    except DruidUtilError as e:
        logger.error(f"Error fetching data from Druid: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error fetching data from Druid. {e}")
    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        raise HTTPException(
            status_code=500, detail=f"Internal Server Error {e}")


@app.get("/get_records_count_aggregated", status_code=status.HTTP_200_OK)
async def get_records_count_aggregated(datasource: str, start_date: str, end_date: str, aggregation_type: str,
                                       device_id: Optional[str] = None):
    logger.info(f"Received request for /get_records_count_aggregated")
    try:
        result = await records_service.get_records_count_aggregated(datasource, device_id, start_date, end_date, aggregation_type)
        return result
    except InvalidDateFormatError as e:
        logger.error(f"Invalid date format: {e}")
        raise HTTPException(
            status_code=400, detail=f"Invalid date format: {e}")
    except DruidUtilError as e:
        logger.error(f"Error fetching data from Druid: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error fetching data from Druid. {e}")
    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        raise HTTPException(
            status_code=500, detail=f"Internal Server Error {e}")


# Processing time-related endpoints
@app.get("/get_processing_time_date_range", status_code=status.HTTP_200_OK)
async def get_processing_time_date_range(datasource: str, start_date: str, end_date: str, metric_type: str,
                                         device_id: Optional[str] = None):
    logger.info(f"Received request for /get_processing_time_date_range")
    try:
        response = await processing_service.get_processing_time_data_range(datasource, device_id, start_date, end_date, metric_type)
        return response
    except InvalidDateFormatError as e:
        logger.error(f"Invalid date format: {e}")
        raise HTTPException(
            status_code=400, detail=f"Invalid date format: {e}")
    except DruidUtilError as e:
        logger.error(f"Error fetching data from Druid: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error fetching data from Druid. {e}")
    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        raise HTTPException(
            status_code=500, detail=f"Internal Server Error {e}")


@app.get("/get_processing_time_aggregated", status_code=status.HTTP_200_OK)
async def get_processing_time_aggregated(datasource: str, start_date: str, end_date: str, aggregation_type: str,
                                         metric_type: str, device_id: Optional[str] = None):
    logger.info(f"Received request for /get_processing_time_aggregated")
    try:
        response = await processing_service.get_processing_time_aggregated(datasource, device_id, start_date, end_date, aggregation_type, metric_type)
        return response
    except InvalidDateFormatError as e:
        logger.error(f"Invalid date format: {e}")
        raise HTTPException(
            status_code=400, detail=f"Invalid date format: {e}")
    except DruidUtilError as e:
        logger.error(f"Error fetching data from Druid: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error fetching data from Druid. {e}")
    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        raise HTTPException(
            status_code=500, detail=f"Internal Server Error {e}")


datasources = ["updates-data6", "kafka-connection-test15"]


# Define root endpoint
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    try:
        return templates.TemplateResponse("index.html", {"request": request, "datasources": datasources})
    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000)
