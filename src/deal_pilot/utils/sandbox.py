import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from weakref import WeakValueDictionary

from langgraph.runtime import get_runtime
from langgraph.store.base import BaseStore
from langgraph.store.memory import InMemoryStore
from opensandbox import Sandbox
from opensandbox.config import ConnectionConfig
from opensandbox.exceptions import SandboxApiException
from opensandbox.models import PVC, Volume

from deal_pilot.config.nacos.nacos_config import get_config
from deal_pilot.config.nacos.nacos_schemas import OpenSandboxConfig
from deal_pilot.config.sandbox_settings import sandbox_settings

_MEMEORY_STORE = InMemoryStore()

_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
_NAMESPACE = ("__sandbox__",)

# store 中共享沙箱实例的固定 key（全员共用一个沙箱 Pod）
SHARED_SANDBOX_KEY = "__deal_pilot_shared_sandbox__"
# 全员共用数据卷 PVC，用户沙箱 /shared 只读，共享沙箱 /shared 可写
SHARED_VOLUME_CLAIM = "deal-pilot-shared"


async def _get_store(store: BaseStore, key: str, field_name: str) -> str | None:
    dict = await store.aget(_NAMESPACE, key)
    if dict is None:
        return None
    return dict.value.get(field_name, None)


def _pvc_volume(name: str, mount_path: str, *, read_only: bool = False) -> Volume:
    return Volume(
        name=name,
        pvc=PVC(
            claimName=name,
            createIfNotExists=True,
            deleteOnSandboxTermination=False,
        ),
        mountPath=mount_path,
        readOnly=read_only,
    )


async def _create(
    *,
    sandbox_config: OpenSandboxConfig,
    connection_config: ConnectionConfig,
    key: str,
    store: BaseStore,
    shared: bool,
) -> Sandbox:
    volumes: list[Volume] = []
    if shared:
        v_name = SHARED_VOLUME_CLAIM
        volumes.append(
            _pvc_volume(SHARED_VOLUME_CLAIM, "/shared", read_only=False)
        )
    else:
        v_name = await _get_store(store, key, "volume")
        if v_name is None:
            v_name = str(uuid.uuid4()).replace("-", "")
        volumes.append(_pvc_volume(v_name, "/root"))
        volumes.append(
            _pvc_volume(SHARED_VOLUME_CLAIM, "/shared", read_only=True)
        )

    sandbox = await Sandbox.create(
        image=sandbox_config.image,
        connection_config=connection_config,
        timeout=timedelta(seconds=sandbox_config.timeout_seconds),
        ready_timeout=timedelta(seconds=sandbox_config.ready_timeout_seconds),
        volumes=volumes,
    )

    await store.aput(_NAMESPACE, key, {"sandbox_id": sandbox.id, "volume": v_name})
    return sandbox


async def _connect(
    *,
    connection_config: ConnectionConfig,
    key: str,
    store: BaseStore,
) -> Sandbox | None:
    "连接已有沙箱"
    sandbox_id = await _get_store(store, key, "sandbox_id")
    if sandbox_id is None:
        return None
    try:
        sandbox = await Sandbox.connect(
            sandbox_id=sandbox_id, connection_config=connection_config
        )
        return sandbox
    except SandboxApiException as exc:
        if exc.status_code == 404:
            return None
        else:
            raise


async def _resolve_store(store: BaseStore | None) -> BaseStore:
    if store is not None:
        return store
    try:
        runtime = get_runtime()
        store = runtime.store
        if store is None:
            return _MEMEORY_STORE
        return store
    except (RuntimeError, AttributeError):
        return _MEMEORY_STORE


async def get_sandbox(
    key: str | None = None,
    store: BaseStore | None = None,
    *,
    shared: bool = False,
):
    if shared:
        key = SHARED_SANDBOX_KEY
    elif key is None:
        key = str(uuid.uuid4())
    store = await _resolve_store(store)
    sandbox_config = await get_config("sandbox_config", OpenSandboxConfig)
    connection_config = ConnectionConfig(
        domain=sandbox_settings.domain,
        api_key=sandbox_settings.api_key,
        use_server_proxy=True,
    )
    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:
        sandbox = await _connect(
            connection_config=connection_config,
            key=key,
            store=store,
        )
        if sandbox:
            return sandbox
        return await _create(
            sandbox_config=sandbox_config,
            connection_config=connection_config,
            key=key,
            store=store,
            shared=shared,
        )


async def get_shared_sandbox(store: BaseStore | None = None):
    return await get_sandbox(store=store, shared=True)


@asynccontextmanager
async def _enter_sandbox(
    key: str | None,
    store: BaseStore | None,
    *,
    shared: bool,
):
    sandbox = await get_sandbox(key=key, store=store, shared=shared)
    sandbox_config = await get_config("sandbox_config", OpenSandboxConfig)
    await sandbox.renew(timedelta(seconds=sandbox_config.timeout_seconds))
    try:
        yield sandbox
    finally:
        await sandbox.renew(timedelta(seconds=sandbox_config.timeout_seconds))
        await sandbox.close()


@asynccontextmanager
async def enter_sandbox(key: str | None = None, store: BaseStore | None = None):
    async with _enter_sandbox(key, store, shared=False) as sandbox:
        yield sandbox


@asynccontextmanager
async def enter_shared_sandbox(store: BaseStore | None = None):
    async with _enter_sandbox(None, store, shared=True) as sandbox:
        yield sandbox
