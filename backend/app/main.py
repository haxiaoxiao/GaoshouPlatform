from fastapi import FastAPI

app = FastAPI(
    title="GaoshouPlatform API",
    description="量化投研平台后端服务",
    version="0.1.0",
)


@app.get("/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/")
async def root():
    """根路径"""
    return {"message": "Welcome to GaoshouPlatform API"}
