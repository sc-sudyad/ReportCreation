# routes/records.py
from typing import Optional

from fastapi import APIRouter, HTTPException, status

from exception_handling.druid_exception import DruidUtilError
from exception_handling.invalid_date_format import InvalidDateFormatError
from services.no_of_records_service import RecordsService
from repo.druid_util import DruidUtil


class RecordsRouter:
    def __init__(self, service: RecordsService):
        self.service = service
        self.router = APIRouter()
        self.router.add_api_route(
            "/get_records_count_date_range",
            self.get_records_count_date_range,
            methods=["GET"],
            summary="Get the number of records for a date range for all devices of a certain type",
            status_code=status.HTTP_200_OK
        )
        self.router.add_api_route(
            "/get_records_count_aggregated",
            self.get_records_count_aggregated,
            methods=["GET"],
            summary="Get the number of records aggregated",
            status_code=status.HTTP_200_OK
        )

    async def get_records_count_date_range(self, device_type: str, start_date: str,
                                           end_date: str, device_id: Optional[str] = None):
        try:
            count = self.service.get_records_count_date_range(device_type, device_id, start_date, end_date)
            return [{"count": count}]
        except InvalidDateFormatError as e:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
        except DruidUtilError as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data from Druid. {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error {e}")

    async def get_records_count_aggregated(self, device_type: str, start_date: str,
                                           end_date: str, aggregation_type: str, device_id: Optional[str] = None):
        try:
            return self.service.get_records_count_aggregated(device_type, device_id, start_date, end_date,
                                                             aggregation_type)
        except InvalidDateFormatError as e:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
        except DruidUtilError as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data from Druid. {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error {e}")


druid_util = DruidUtil()
records_service = RecordsService(druid_util=druid_util)
records_router = RecordsRouter(service=records_service)
no_of_records = records_router.router
