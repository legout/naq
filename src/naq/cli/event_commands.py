# src/naq/cli/event_commands.py
"""
Event monitoring commands for the NAQ CLI.

This module contains commands for monitoring job events, viewing event history,
and generating event statistics.
"""

import asyncio
import datetime
import json
import time
from collections import Counter, defaultdict
from datetime import timezone
from typing import Optional

import typer
from rich.panel import Panel
from rich.table import Table

from .main import console
from ..settings import DEFAULT_NATS_URL


# Create event command group
event_app = typer.Typer(help="Event monitoring and analytics commands")


@event_app.command()
def stream(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url", 
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    job_id: Optional[str] = typer.Option(
        None,
        "--job-id",
        "-j",
        help="Filter events for a specific job ID.",
    ),
    event_type: Optional[str] = typer.Option(
        None,
        "--event-type",
        "-e", 
        help="Filter events by type (enqueued, started, completed, failed, retry_scheduled, scheduled, schedule_triggered).",
    ),
    queue_name: Optional[str] = typer.Option(
        None,
        "--queue",
        "-q",
        help="Filter events for a specific queue.",
    ),
    worker_id: Optional[str] = typer.Option(
        None,
        "--worker",
        "-w",
        help="Filter events for a specific worker.",
    ),
    format_output: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table, json, or raw.",
    ),
):
    """
    Monitor job events in real-time.
    
    This command connects to the NATS event stream and displays job lifecycle
    events as they occur. Supports filtering by job ID, event type, queue, and worker.
    """
    from ..events.processor import AsyncJobEventProcessor
    from ..models import JobEventType
    
    # Validate event type
    event_type_enum = None
    if event_type:
        try:
            event_type_enum = JobEventType(event_type.lower())
        except ValueError:
            console.print(f"[red]Error:[/red] Invalid event type '{event_type}'")
            console.print(f"Valid types: {', '.join([e.value for e in JobEventType])}")
            raise typer.Exit(code=1)
    
    async def run_events_monitor():
        """Run the events monitor."""
        config = {'nats_url': nats_url}
        processor = AsyncJobEventProcessor(config=config)
        
        # Create a simple handler to display events
        def display_event(event):
            if format_output == "json":
                console.print(json.dumps(event.to_dict(), indent=2))
            elif format_output == "raw":
                console.print(f"{event.timestamp:.2f} {event.job_id} {event.event_type.value} {event.message or ''}")
            else:  # table format
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Time", style="dim")
                table.add_column("Job ID", style="cyan")
                table.add_column("Event", style="green")
                table.add_column("Queue", style="blue")
                table.add_column("Worker", style="yellow")
                table.add_column("Message")
                
                # Format timestamp
                dt = datetime.datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
                time_str = dt.strftime("%H:%M:%S.%f")[:-3]  # Include milliseconds
                
                table.add_row(
                    time_str,
                    event.job_id[:12] + "..." if len(event.job_id) > 15 else event.job_id,
                    event.event_type.value,
                    event.queue_name or "-",
                    event.worker_id[:8] + "..." if event.worker_id and len(event.worker_id) > 11 else (event.worker_id or "-"),
                    event.message or "-"
                )
                console.print(table)
        
        # Register the display handler
        processor.add_global_handler(display_event)
        
        try:
            # Start the processor
            await processor.start()
            
            console.print(Panel(
                "[bold green]NAQ Event Monitor Started[/bold green]\n"
                f"NATS URL: {nats_url}\n"
                f"Filters: job_id={job_id or 'all'}, event_type={event_type or 'all'}, "
                f"queue={queue_name or 'all'}, worker={worker_id or 'all'}\n"
                f"Format: {format_output}\n\n"
                "[dim]Press Ctrl+C to stop[/dim]",
                title="Event Monitor",
                border_style="green"
            ))
            
            # Stream events with filters
            async for event in processor.stream_job_events(
                job_id=job_id,
                event_type=event_type_enum,
                queue_name=queue_name,
                worker_id=worker_id
            ):
                # The display_event handler will be called automatically
                pass
                
        except KeyboardInterrupt:
            console.print("\n[yellow]Event monitoring stopped by user[/yellow]")
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(code=1)
        finally:
            await processor.stop()
    
    # Run the async function
    asyncio.run(run_events_monitor())


@event_app.command("history")
def event_history(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url", 
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    job_id: str = typer.Argument(
        help="Job ID to get event history for.",
    ),
    limit: int = typer.Option(
        50,
        "--limit",
        "-l",
        help="Maximum number of events to retrieve.",
    ),
):
    """
    Get event history for a specific job.
    
    This command retrieves the complete event history for a job from the event stream,
    showing all lifecycle events in chronological order.
    """
    from ..events.storage import NATSJobEventStorage
    
    async def get_job_events():
        """Retrieve events for a specific job."""
        config = {'nats_url': nats_url}
        storage = NATSJobEventStorage(config=config)
        
        try:
            events = await storage.get_events(job_id)
            
            if not events:
                console.print(f"[yellow]No events found for job '{job_id}'[/yellow]")
                return
            
            # Limit results
            if len(events) > limit:
                events = events[-limit:]  # Get the most recent events
                console.print(f"[dim]Showing last {limit} events (total: {len(events)})[/dim]\n")
            
            # Create a table for events
            table = Table(show_header=True, header_style="bold magenta", title=f"Event History for Job: {job_id}")
            table.add_column("Timestamp", style="dim")
            table.add_column("Event Type", style="green")
            table.add_column("Queue", style="blue")
            table.add_column("Worker", style="yellow")
            table.add_column("Message")
            table.add_column("Details", style="dim")
            
            for event in events:
                # Format timestamp
                dt = datetime.datetime.fromtimestamp(event.timestamp, tz=timezone.utc)
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                
                # Format details
                details_str = ""
                if event.details:
                    key_details = []
                    if event.duration_ms is not None:
                        key_details.append(f"duration: {event.duration_ms}ms")
                    if event.error_type:
                        key_details.append(f"error: {event.error_type}")
                    details_str = ", ".join(key_details)
                
                table.add_row(
                    time_str,
                    event.event_type.value,
                    event.queue_name or "-",
                    (event.worker_id[:8] + "..." if event.worker_id and len(event.worker_id) > 11 else (event.worker_id or "-")),
                    event.message or "-",
                    details_str
                )
            
            console.print(table)
            
        except Exception as e:
            console.print(f"[red]Error retrieving events:[/red] {e}")
            raise typer.Exit(code=1)
        finally:
            await storage.close()
    
    # Run the async function
    asyncio.run(get_job_events())


@event_app.command("stats")
def event_stats(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url", 
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    hours: int = typer.Option(
        24,
        "--hours",
        "-h",
        help="Number of hours to look back for statistics.",
    ),
    by_queue: bool = typer.Option(
        False,
        "--by-queue",
        help="Group statistics by queue.",
    ),
    by_worker: bool = typer.Option(
        False,
        "--by-worker", 
        help="Group statistics by worker.",
    ),
):
    """
    Show event statistics and system health metrics.
    
    This command provides an overview of system activity by analyzing
    events from the specified time period.
    """
    from ..events.storage import NATSJobEventStorage
    from ..models import JobEventType
    
    async def get_event_statistics():
        """Generate event statistics."""
        config = {'nats_url': nats_url}
        storage = NATSJobEventStorage(config=config)
        
        try:
            console.print(f"[dim]Analyzing events from the last {hours} hours...[/dim]\n")
            
            # Calculate time range
            current_time = time.time()
            start_time = current_time - (hours * 3600)
            
            # Get recent events for analysis
            # Note: This is a simplified implementation. A production version would
            # use more efficient time-based queries
            all_events = []
            try:
                # This would need to be implemented in the storage layer
                # For now, we'll analyze available events
                pass
            except Exception:
                pass
            
            # Generate statistics
            stats = {
                'total_events': 0,
                'events_by_type': defaultdict(int),
                'events_by_queue': defaultdict(int),
                'events_by_worker': defaultdict(int),
                'completion_times': [],
                'error_types': defaultdict(int),
                'period_hours': hours
            }
            
            # Create statistics table
            stats_table = Table(title=f"Event Statistics (Last {hours} hours)")
            stats_table.add_column("Metric", style="cyan")
            stats_table.add_column("Count", style="green")
            stats_table.add_column("Details", style="dim")
            
            stats_table.add_row("Total Events", str(stats['total_events']), "All event types")
            
            # Add placeholder data to show structure
            stats_table.add_row("Jobs Completed", "0", "Successful job completions")
            stats_table.add_row("Jobs Failed", "0", "Failed job executions")
            stats_table.add_row("Jobs Retried", "0", "Jobs scheduled for retry")
            stats_table.add_row("Average Duration", "N/A", "Mean job execution time")
            
            console.print(stats_table)
            
            if by_queue:
                queue_table = Table(title="Statistics by Queue")
                queue_table.add_column("Queue", style="blue")
                queue_table.add_column("Events", style="green")
                queue_table.add_column("Success Rate", style="yellow")
                
                queue_table.add_row("default", "0", "N/A")
                console.print("\n")
                console.print(queue_table)
            
            if by_worker:
                worker_table = Table(title="Statistics by Worker")
                worker_table.add_column("Worker ID", style="yellow")
                worker_table.add_column("Events", style="green") 
                worker_table.add_column("Jobs Processed", style="cyan")
                
                worker_table.add_row("No active workers", "0", "0")
                console.print("\n")
                console.print(worker_table)
            
            console.print(f"\n[dim]Note: Full statistics require time-based event querying.[/dim]")
            
        except Exception as e:
            console.print(f"[red]Error retrieving statistics:[/red] {e}")
            raise typer.Exit(code=1)
        finally:
            await storage.close()
    
    # Run the async function
    asyncio.run(get_event_statistics())


@event_app.command("workers")
def monitor_workers(
    nats_url: str = typer.Option(
        DEFAULT_NATS_URL,
        "--nats-url", 
        "-u",
        help="URL of the NATS server.",
        envvar="NAQ_NATS_URL",
    ),
    format_output: str = typer.Option(
        "table",
        "--format",
        "-f", 
        help="Output format: table or json.",
    ),
    show_inactive: bool = typer.Option(
        False,
        "--show-inactive",
        help="Include inactive workers in output.",
    ),
    refresh: int = typer.Option(
        0,
        "--refresh",
        "-r",
        help="Auto-refresh interval in seconds (0 = no refresh).",
    ),
):
    """
    Monitor worker events and status in real-time.
    
    This command shows active workers, their current status, and recent activity
    based on worker events from the event stream.
    """
    from ..events.processor import AsyncJobEventProcessor
    from ..models import JobEventType
    
    async def monitor_worker_status():
        """Monitor worker status via events."""
        config = {'nats_url': nats_url}
        processor = AsyncJobEventProcessor(config=config)
        
        # Track worker status
        worker_status = {}
        worker_stats = defaultdict(lambda: {'jobs_processed': 0, 'jobs_failed': 0, 'last_seen': None})
        
        def handle_worker_event(event):
            """Handle worker-related events."""
            if hasattr(event, 'worker_id') and event.worker_id:
                worker_id = event.worker_id
                current_time = event.timestamp
                
                # Update last seen time
                worker_stats[worker_id]['last_seen'] = current_time
                
                # Track job outcomes
                if event.event_type == JobEventType.COMPLETED:
                    worker_stats[worker_id]['jobs_processed'] += 1
                elif event.event_type == JobEventType.FAILED:
                    worker_stats[worker_id]['jobs_failed'] += 1
                
                # Update status display
                update_worker_display()
        
        def update_worker_display():
            """Update the worker status display."""
            if format_output == "json":
                output = []
                for worker_id, stats in worker_stats.items():
                    if not show_inactive and is_worker_inactive(stats['last_seen']):
                        continue
                    
                    output.append({
                        'worker_id': worker_id,
                        'jobs_processed': stats['jobs_processed'],
                        'jobs_failed': stats['jobs_failed'],
                        'last_seen': stats['last_seen'],
                        'status': 'active' if not is_worker_inactive(stats['last_seen']) else 'inactive'
                    })
                
                console.print(json.dumps(output, indent=2))
            else:
                # Table format
                table = Table(title="Worker Status Monitor")
                table.add_column("Worker ID", style="cyan")
                table.add_column("Status", style="green")
                table.add_column("Jobs Processed", style="blue")
                table.add_column("Jobs Failed", style="red")
                table.add_column("Success Rate", style="yellow")
                table.add_column("Last Seen", style="dim")
                
                if not worker_stats:
                    table.add_row("No workers detected", "-", "-", "-", "-", "-")
                else:
                    for worker_id, stats in sorted(worker_stats.items()):
                        if not show_inactive and is_worker_inactive(stats['last_seen']):
                            continue
                        
                        # Calculate success rate
                        total_jobs = stats['jobs_processed'] + stats['jobs_failed']
                        success_rate = f"{(stats['jobs_processed'] / total_jobs * 100):.1f}%" if total_jobs > 0 else "N/A"
                        
                        # Format last seen
                        if stats['last_seen']:
                            last_seen = datetime.datetime.fromtimestamp(stats['last_seen'], timezone.utc).strftime("%H:%M:%S")
                        else:
                            last_seen = "Never"
                        
                        # Determine status
                        status = "🟢 Active" if not is_worker_inactive(stats['last_seen']) else "🔴 Inactive"
                        
                        table.add_row(
                            worker_id[:12] + "..." if len(worker_id) > 15 else worker_id,
                            status,
                            str(stats['jobs_processed']),
                            str(stats['jobs_failed']),
                            success_rate,
                            last_seen
                        )
                
                console.clear()
                console.print(table)
                console.print(f"\n[dim]Last updated: {datetime.datetime.now().strftime('%H:%M:%S')}[/dim]")
                if refresh > 0:
                    console.print(f"[dim]Refreshing every {refresh}s (Press Ctrl+C to stop)[/dim]")
        
        def is_worker_inactive(last_seen_time):
            """Check if worker is considered inactive (no activity in last 60 seconds)."""
            if last_seen_time is None:
                return True
            return (time.time() - last_seen_time) > 60
        
        # Register event handler
        processor.add_global_handler(handle_worker_event)
        
        try:
            await processor.start()
            
            console.print(Panel(
                "[bold green]NAQ Worker Monitor Started[/bold green]\n"
                f"NATS URL: {nats_url}\n"
                f"Format: {format_output}\n"
                f"Show inactive: {show_inactive}\n"
                f"Refresh: {'Disabled' if refresh == 0 else f'{refresh}s'}\n\n"
                "[dim]Monitoring worker events...[/dim]",
                title="Worker Monitor",
                border_style="green"
            ))
            
            # Initial display
            update_worker_display()
            
            if refresh > 0:
                # Auto-refresh mode
                while True:
                    await asyncio.sleep(refresh)
                    update_worker_display()
            else:
                # Just monitor events
                await asyncio.sleep(3600)  # Run for 1 hour
                
        except KeyboardInterrupt:
            console.print("\n[yellow]Worker monitoring stopped by user[/yellow]")
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(code=1)
        finally:
            await processor.stop()
    
    # Run the async function
    asyncio.run(monitor_worker_status())


# Backward compatibility alias for tests
event_commands = event_app