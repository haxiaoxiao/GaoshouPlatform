"""Factor CRUD, templates, and validation API endpoints."""

from fastapi import APIRouter, HTTPException

from app.models.factor import (
    FactorTemplate,
    FactorCreate,
    FactorUpdate,
    FactorResponse,
    ValidateRequest,
    ValidateResponse,
)
from app.services.factor_templates import FactorTemplatesService
from app.services.factor_validator import FactorValidator

router = APIRouter(prefix="/v2/factors", tags=["因子管理"])

templates_service = FactorTemplatesService()
validator = FactorValidator()


@router.get("/templates", response_model=list[FactorTemplate])
async def list_templates():
    """List all factor creation templates."""
    return templates_service.list_templates()


@router.post("/validate", response_model=ValidateResponse)
async def validate_expression(req: ValidateRequest):
    """Validate a factor expression."""
    result = validator.validate(req.expression)
    return ValidateResponse(**result)


@router.post("/create", response_model=FactorResponse)
async def create_factor(data: FactorCreate):
    """Create a new factor. DB persistence not yet implemented."""
    raise HTTPException(status_code=501, detail="Factor persistence not yet implemented")


@router.get("/{factor_id}", response_model=FactorResponse)
async def get_factor(factor_id: int):
    """Get factor by ID. DB persistence not yet implemented."""
    raise HTTPException(status_code=501, detail="Factor persistence not yet implemented")


@router.put("/{factor_id}", response_model=FactorResponse)
async def update_factor(factor_id: int, data: FactorUpdate):
    """Update factor expression or parameters. DB persistence not yet implemented."""
    raise HTTPException(status_code=501, detail="Factor persistence not yet implemented")


@router.delete("/{factor_id}")
async def delete_factor(factor_id: int):
    """Delete a factor. DB persistence not yet implemented."""
    raise HTTPException(status_code=501, detail="Factor persistence not yet implemented")
