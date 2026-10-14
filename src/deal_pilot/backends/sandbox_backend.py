import asyncio
from collections.abc import Callable
from datetime import timedelta

from deepagents.backends.protocol import (
    DeleteResult,
    ExecuteResponse,
    FileDownloadResponse,
    FileUploadResponse,
)
from deepagents.backends.sandbox import BaseSandbox
from opensandbox.exceptions import SandboxException
from opensandbox.models.execd import RunCommandOpts

from deal_pilot.config.nacos.nacos_config import get_config
from deal_pilot.config.nacos.nacos_schemas import OpenSandboxConfig
from deal_pilot.utils.sandbox import enter_sandbox
from deal_pilot.utils.shared import get_user_id

type IdGetter = Callable[[], str]


class SandboxBackend(BaseSandbox):
    """Deep Agents 文件/命令后端，仅连接当前用户的沙箱（/shared 只读）。"""

    id_getter: IdGetter

    def __init__(self, id_getter: IdGetter | None = None) -> None:
        super().__init__()
        if not id_getter:
            id_getter = get_user_id
        self.id_getter = id_getter

    @property
    def id(self) -> str:
        return self.id_getter()

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        return asyncio.run(self.aexecute(command, timeout=timeout))

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        return asyncio.run(self.aupload_files(files))

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        return asyncio.run(self.adownload_files(paths))

    async def aexecute(
        self, command: str, *, timeout: int | None = None
    ) -> ExecuteResponse:
        config = await get_config("sandbox_config", OpenSandboxConfig)
        if timeout is None or timeout < 0:
            timeout = config.command_timeout_seconds
        async with enter_sandbox(self.id) as sandbox:
            execution = await sandbox.commands.run(
                command, opts=RunCommandOpts(timeout=timedelta(seconds=timeout))
            )
        return ExecuteResponse(
            output=str(execution), exit_code=execution.exit_code, truncated=False
        )

    async def aupload_files(
        self, files: list[tuple[str, bytes]]
    ) -> list[FileUploadResponse]:
        responses: list[FileUploadResponse] = []
        async with enter_sandbox(self.id) as sandbox:
            for path, data in files:
                try:
                    await sandbox.files.write_file(path, data)
                    responses.append(FileUploadResponse(path=path))
                except SandboxException as exc:
                    responses.append(FileUploadResponse(path=path, error=str(exc)))
        return responses

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        responses: list[FileDownloadResponse] = []
        async with enter_sandbox(self.id) as sandbox:
            for path in paths:
                try:
                    content = await sandbox.files.read_bytes(path)
                    responses.append(FileDownloadResponse(path=path, content=content))
                except SandboxException as exc:
                    responses.append(FileDownloadResponse(path=path, error=str(exc)))
        return responses

    async def adelete(self, file_path: str) -> DeleteResult:
        result = await self.aexecute(f"rm -rf -- {file_path}")
        if result.exit_code == 0:
            return DeleteResult(path=file_path)
        return DeleteResult(
            error=f"删除路径{file_path}失败，原因是:{result.output}\n退出码：{result.exit_code}"
        )
