import pytest
import pytest_asyncio
import asyncio
import time
from typing import Any, Dict

import nats
from nats.js.errors import KeyNotFoundError, BucketNotFoundError

from naq.services.kv_stores import KVStoreService, KVStoreServiceConfig
from naq.services.connection import ConnectionService
from naq.services.base import ServiceConfig
from naq.exceptions import NaqException


@pytest_asyncio.fixture
async def nats_client(nats_server):
    """Create a NATS client for direct KV operations."""
    nc = await nats.connect(nats_server)
    try:
        yield nc
    finally:
        await nc.close()


@pytest.mark.asyncio
async def test_direct_kv_operations(nats_client):
    """Test direct NATS KV operations to verify NATS is working."""
    js = nats_client.jetstream()
    
    # Create a KV bucket
    kv = await js.create_key_value(
        bucket="test_direct",
        description="Test bucket for direct operations"
    )
    
    # Put a value
    await kv.put("test_key", b"test_value")
    
    # Get the value
    entry = await kv.get("test_key")
    assert entry.value == b"test_value"
    
    # Delete the key
    await kv.delete("test_key")
    
    # Verify it's gone
    with pytest.raises(KeyNotFoundError):
        await kv.get("test_key")


@pytest.mark.asyncio
async def test_connection_service(nats_server):
    """Test ConnectionService directly."""
    config = ServiceConfig(nats_url=nats_server)
    service = ConnectionService(config=config)
    
    try:
        await service.initialize()
        
        # Get JetStream context
        js = await service.get_jetstream()
        
        # Create a KV bucket directly
        kv = await js.create_key_value(
            bucket="test_connection",
            description="Test bucket for connection service"
        )
        
        # Put a value
        await kv.put("test_key", b"test_value")
        
        # Get the value
        entry = await kv.get("test_key")
        assert entry.value == b"test_value"
        
    finally:
        await service.cleanup()


@pytest.mark.asyncio
async def test_kv_service_simple(nats_server):
    """Test KVStoreService with direct NATS client."""
    # Create direct NATS client
    nc = await nats.connect(nats_server)
    
    try:
        config = ServiceConfig(nats_url=nats_server)
        kv_config = KVStoreServiceConfig()
        kv_service = KVStoreService(config=config, nats_client=nc)
        
        try:
            await kv_service.initialize()
            
            # Simple put/get test
            await kv_service.put("test_simple", "key", "value")
            result = await kv_service.get("test_simple", "key")
            assert result == "value"
            
        finally:
            await kv_service.cleanup()
            
    finally:
        await nc.close()