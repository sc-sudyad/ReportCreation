# routes/records.py
from typing import Optional

from fastapi import APIRouter, HTTPException, status

from exception_handling.druid_exception import DruidUtilError
from exception_handling.invalid_date_format import InvalidDateFormatError
from services.processing_time_service import ProcessingTimeService
from repo.druid_util import DruidUtil


class ProcessingTimeRouter:
    def __init__(self, service: ProcessingTimeService):
        self.service = service
        self.router = APIRouter()
        self.router.add_api_route(
            "/get_processing_time",
            self.get_processing_time,
            methods=["GET"],
            summary="Processing Time for All Devices by Device Type",
            status_code=status.HTTP_200_OK
        ),
        self.router.add_api_route(
            "/get_processing_time_date_range",
            self.get_processing_time_date_range,
            methods=["GET"],
            summary="Processing Time for All Devices Over a Date Range",
            status_code=status.HTTP_200_OK
        ),
        self.router.add_api_route(
            "/get_processing_time_aggregated",
            self.get_processing_time_aggregated,
            methods=["GET"],
            summary="Aggregated Processing Time",
            status_code=status.HTTP_200_OK
        )

    async def get_processing_time(self, device_type: str, metric_type: str, device_id: Optional[str] = None):
        try:
            return self.service.get_processing_time(device_type, device_id, metric_type)
        except DruidUtilError as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data from Druid. {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error {e}")

    async def get_processing_time_date_range(self, device_type: str, start_date: str,
                                             end_date: str, metric_type: str, device_id: Optional[str] = None):
        try:
            return self.service.get_processing_time_data_range(device_type, device_id, start_date, end_date,
                                                               metric_type)
        except InvalidDateFormatError as e:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
        except DruidUtilError as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data from Druid. {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error {e}")

    async def get_processing_time_aggregated(self, device_type: str, start_date: str,
                                             end_date: str, aggregation_type: str, metric_type: str,
                                             device_id: Optional[str] = None):
        try:
            return self.service.get_processing_time_aggregated(device_type, device_id, start_date, end_date,
                                                               aggregation_type, metric_type)
        except InvalidDateFormatError as e:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
        except DruidUtilError as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data from Druid. {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error {e}")


druid_util = DruidUtil()
processing_service = ProcessingTimeService(druid_util=druid_util)
processing_router = ProcessingTimeRouter(service=processing_service)
common_processing_router = processing_router.router
