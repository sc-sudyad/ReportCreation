# routes/kafka.py
from typing import Optional

from fastapi import APIRouter, HTTPException, status
from services.record_type_service import RecordTypeService
from repo.kafka_util import KafkaUtil
from repo.druid_util import DruidUtil


class RecordTypeRoute:
    def __init__(self, service: RecordTypeService):
        self.service = service
        self.router = APIRouter()
        self.router.add_api_route(
            "/get_processing_stats",
            self.get_processing_stats,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            summary="Get all devices record count"
        ),
        self.router.add_api_route(
            "/get_summary",
            self.get_summary,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            summary="Get summary"
        )

    async def get_processing_stats(self, device_type: str, device_id: Optional[str] = None):
        try:
            return self.service.get_processing_stats(device_type, device_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def get_summary(self, device_type: str):
        try:
            return self.service.get_summary(device_type)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


druid_util = DruidUtil()
kafka_util = KafkaUtil()
kafka_service = RecordTypeService(kafka_util=kafka_util, druid_util=druid_util)
kafka_router = RecordTypeRoute(service=kafka_service)
record_router = kafka_router.router
