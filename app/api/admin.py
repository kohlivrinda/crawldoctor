"""Admin API endpoints for CrawlDoctor."""
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
import structlog

from app.database import get_db
from app.models.user import User
from app.models.visit import Visit, VisitSession
from app.services.backfill import BackfillService
from app.utils.auth import get_current_user, require_permission

logger = structlog.get_logger()

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/stats")
async def get_admin_stats(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get admin dashboard statistics."""
    require_permission(current_user, "admin")
    
    try:
        # Get basic stats
        total_visits = db.query(func.count(Visit.id)).scalar()
        total_sessions = db.query(func.count(VisitSession.id)).scalar()
        total_users = db.query(func.count(User.id)).scalar()
        
        # Get recent activity (last 24 hours)
        yesterday = datetime.now() - timedelta(days=1)
        recent_visits = db.query(func.count(Visit.id)).filter(
            Visit.timestamp >= yesterday
        ).scalar()
        
        # Get crawler breakdown
        crawler_stats = db.query(
            Visit.crawler_type,
            func.count(Visit.id).label('count')
        ).filter(
            Visit.is_bot == True
        ).group_by(Visit.crawler_type).order_by(
            func.count(Visit.id).desc()
        ).limit(10).all()
        
        return {
            "total_visits": total_visits,
            "total_sessions": total_sessions,
            "total_users": total_users,
            "recent_visits_24h": recent_visits,
            "crawler_breakdown": [
                {"crawler": stat.crawler_type, "count": stat.count}
                for stat in crawler_stats
            ]
        }
        
    except Exception as e:
        logger.error("Failed to get admin stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get admin statistics")


@router.get("/users")
async def get_users(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of all users."""
    require_permission(current_user, "admin")
    
    try:
        users = db.query(User).all()
        return [
            {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "full_name": user.full_name,
                "is_active": user.is_active,
                "is_superuser": user.is_superuser,
                "created_at": user.created_at.isoformat() if user.created_at else None
            }
            for user in users
        ]
        
    except Exception as e:
        logger.error("Failed to get users", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get users")


@router.get("/recent-activity")
async def get_recent_activity(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get recent system activity."""
    require_permission(current_user, "admin")
    
    try:
        recent_visits = db.query(Visit).order_by(
            Visit.timestamp.desc()
        ).limit(limit).all()
        
        return [
            {
                "id": visit.id,
                "timestamp": visit.timestamp.isoformat(),
                "ip_address": visit.ip_address,
                "user_agent": visit.user_agent[:100],
                "page_url": visit.page_url,
                "crawler_type": visit.crawler_type,
                "is_bot": visit.is_bot,
                "country": visit.country,
                "city": visit.city
            }
            for visit in recent_visits
        ]
    except Exception as e:
        logger.error("Failed to get recent activity", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get recent activity")


@router.post("/rebuild-summaries")
async def rebuild_summaries(
    days: int = Query(90, ge=1, le=365),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Rebuild pre-computed lead and journey summaries."""
    require_permission(current_user, "admin")
    
    try:
        service = BackfillService()
        result = service.backfill_all(db, days=days)
        return {
            "status": "success",
            "message": f"Successfully rebuilt {result['journeys']} journeys and {result['leads']} leads.",
            "details": result
        }
    except Exception as e:
        logger.error("Failed to rebuild summaries", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

