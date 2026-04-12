# backend/app/api/system.py
from fastapi import APIRouter

router = APIRouter()


@router.get("/status")
async def get_system_status():
    """获取系统状态"""
    return {
        "status": "running",
        "database": "connected",
    }


@router.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy"}
