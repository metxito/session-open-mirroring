from utils.open_mirroing_client import OpenMirroringClient

from sqlalchemy import text

import threading
import time
import sqlalchemy as sa

from fastapi import APIRouter, Request, Form, BackgroundTasks
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import JSONResponse
from config import Config
from services.data_service import (
    engine_control, get_queries, get_connection_names, get_sources, get_source_columns, run_simulation_procedure, activate_query, deactivate_query, restart_query, get_query
)
from services.onelake_service import onelake_delete_table_folder


router = APIRouter()
templates = Jinja2Templates(directory="templates")


# New /simulation route
@router.get("/simulation")
def simulation(request: Request):
    return templates.TemplateResponse("simulation.html", {"request": request})


@router.get("/")
def root():
    return RedirectResponse(url="/current_status")


@router.get("/current_status")
def current_status(request: Request):
    queries = get_queries()
    connection_names = get_connection_names()
    sim_running = simulation_state["running"]
    return templates.TemplateResponse("current_status.html", {
        "request": request,
        "queries": queries,
        "connection_names": connection_names,
        "sim_running": sim_running
    })



@router.post("/toggle_active/{query_id}")
def toggle_active(query_id: int):
    with engine_control.begin() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET [active] = CASE WHEN [active] = 1 THEN 0 ELSE 1 END WHERE [id] = :id"), {"id": query_id})
    return RedirectResponse(url="/current_status", status_code=303)

@router.post("/restart/{query_id}")
def restart(query_id: int):
    try:
        deactivate_query(query_id)

        q = get_query(query_id)
        onelake_delete_table_folder(q["schema"], q["table"])

        restart_query(query_id)
        activate_query(query_id)

    
    except Exception as e:
        print (str(e))
        
    return RedirectResponse(url="/current_status", status_code=303)



@router.get("/integration")
def integration(request: Request):
    sources = get_sources()
    return templates.TemplateResponse("integration.html", {
        "request": request,
        "sources": sources
    })

@router.post("/refresh_sources")
def refresh_sources():
    with engine_control.begin() as conn:
        conn.execute(text("EXEC [control].[usp_refresh_metadata]"))
    return RedirectResponse(url="/integration", status_code=303)

@router.post("/delete_query/{query_id}")
def delete_query(query_id: int):
    with engine_control.begin() as conn:
        conn.execute(text("DELETE FROM [control].[queries_control] WHERE [id] = :id"), {"id": query_id})
    return RedirectResponse(url="/current_status", status_code=303)

@router.post("/edit_query/{query_id}")
def edit_query(
    query_id: int,
    query_incremental: str = Form(...),
    unique_keys: str = Form(...)
):
    with engine_control.begin() as conn:
        conn.execute(text(
            "UPDATE [control].[queries_control] SET [query_incremental] = :query_incremental, [unique_keys] = :unique_keys WHERE [id] = :id"),
            {
                "query_incremental": query_incremental,
                "unique_keys": unique_keys,
                "id": query_id
            }
        )
    return RedirectResponse(url="/current_status", status_code=303)

@router.post("/include_source/{source_id}")
def include_source(source_id: int):
    from services.data_service import engine_control
    from sqlalchemy import text
    with engine_control.begin() as conn:
        conn.execute(text("EXEC [control].[usp_add_source_object] @id=:source_id"), {"source_id": source_id})
    return RedirectResponse(url="/current_status", status_code=303)

@router.get("/update_columns/{id}", response_class=HTMLResponse)
def update_columns_form(request: Request, id: int):
    columns = get_source_columns(id)
    return templates.TemplateResponse("update_columns.html", {
        "request": request,
        "source_id": id,
        "columns": columns
    })

@router.post("/update_columns/{tableid}")
def update_columns(request: Request, tableid: int, unique_key: list[int] = Form([])):
    from services.data_service import engine_control
    from sqlalchemy import text
    # unique_key and timestamp_key are lists of column ids to set as True
    with engine_control.begin() as conn:
        # First, set all to 0 for this source
        conn.execute(text("UPDATE c SET [unique_key]=0 FROM [source].[columns] AS c JOIN [source].[sources] AS s ON c.[schema]=s.[schema] AND c.[table]=s.[table] WHERE s.[id]=:tableid"), {"tableid": tableid})
        # Then, set selected unique_key columns to 1
        if unique_key:
            for i in unique_key:
                conn.execute(text("UPDATE [source].[columns] SET [unique_key]=1 WHERE [id] = :i"), {"i": i})
    return RedirectResponse(url="/integration", status_code=303)

@router.get("/check_connection")
def check_connection_form(request: Request):
    return templates.TemplateResponse("check_connection.html", {"request": request})

@router.post("/check_connection")
def check_connection(request: Request):
    from services.data_service import engine_control
    from sqlalchemy import text
    try:
        with engine_control.begin() as conn:
            conn.execute(text("SELECT 1"))
        return templates.TemplateResponse("check_connection.html", {"request": request, "status": "success"})
    except Exception as e:
        return templates.TemplateResponse("check_connection.html", {"request": request, "status": "error", "error": str(e)})






# Simulation state and thread
simulation_state = {"running": False, "thread": None, "logs": []}
def simulation_loop():
    while simulation_state["running"]:
        try:
            current_timestamp = run_simulation_procedure()
            simulation_state["logs"].append(current_timestamp)
        except Exception as e:
            print(f"Simulation error: {e}")
        for _ in range(2):
            if not simulation_state["running"]:
                break
            time.sleep(1)


@router.get("/simulation_status")
def simulation_status():
    # Collect unsent logs
    unsent_logs = simulation_state["logs"]
    simulation_state["logs"] = []

    # Mark them as sent
    if unsent_logs and len(unsent_logs) > 0:
        unsent_logs_text = [x.strftime("%Y-%m-%d %H:%M:%S") for x in unsent_logs]
        return JSONResponse({
            "running": simulation_state["running"],
            "new_logs": unsent_logs_text
        })
    else:
        return JSONResponse({
            "running": simulation_state["running"]
        })


@router.post("/start_simulation")
def start_simulation():
    if not simulation_state["running"]:
        simulation_state["running"] = True
        t = threading.Thread(target=simulation_loop, daemon=True)
        simulation_state["thread"] = t
        t.start()
    return JSONResponse({"status": "started"})


@router.post("/stop_simulation")
def stop_simulation():
    simulation_state["running"] = False
    return JSONResponse({"status": "stopped"})







# --- Call Loop Endpoints ---
@router.get("/callloop_status")
def callloop_status():
    status = OpenMirroringClient.get_call_loop_status()
    return JSONResponse(status)

@router.post("/start_callloop")
def start_callloop():
    OpenMirroringClient.start_call_loop()
    return JSONResponse({"status": "started"})

@router.post("/stop_callloop")
def stop_callloop():
    OpenMirroringClient.stop_call_loop()
    return JSONResponse({"status": "stopped"})







# Register router with FastAPI app
def register_routes(app):
    app.include_router(router)





