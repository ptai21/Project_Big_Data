from supabase import Client
from typing import Optional, Dict, Any
from datetime import datetime


class LogRepository:
    """Repository for logging to Supabase"""
    
    def __init__(self, supabase: Client):
        self.supabase = supabase

    async def log_api_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        response_time_ms: float,
        user_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
        request_params: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log API request to Supabase"""
        try:
            self.supabase.table("api_request_log").insert({
                "endpoint": endpoint,
                "method": method,
                "status_code": status_code,
                "response_time_ms": response_time_ms,
                "user_ip": user_ip,
                "user_agent": user_agent,
                "request_params": request_params,
                "created_at": datetime.utcnow().isoformat()
            }).execute()
        except Exception as e:
            # Don't break the app if logging fails
            print(f"Failed to log API request: {e}")

    async def log_error(
        self,
        error_type: str,
        error_message: str,
        endpoint: Optional[str] = None,
        stack_trace: Optional[str] = None
    ) -> None:
        """Log error to Supabase"""
        try:
            self.supabase.table("error_log").insert({
                "error_type": error_type,
                "error_message": error_message,
                "endpoint": endpoint,
                "stack_trace": stack_trace,
                "created_at": datetime.utcnow().isoformat()
            }).execute()
        except Exception as e:
            print(f"Failed to log error: {e}")

    async def log_process(
        self,
        process_name: str,
        status: str,  # "started", "completed", "failed"
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log background process to Supabase"""
        try:
            self.supabase.table("process_log").insert({
                "process_name": process_name,
                "status": status,
                "details": details,
                "created_at": datetime.utcnow().isoformat()
            }).execute()
        except Exception as e:
            print(f"Failed to log process: {e}")
