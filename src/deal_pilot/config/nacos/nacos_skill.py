import asyncio
import io
import zipfile
from dataclasses import dataclass
from pathlib import PurePosixPath

import httpx

from deal_pilot.config.nacos.nacos_access_token import get_access_token
from deal_pilot.config.nacos_settings import nacos_settings
from deal_pilot.utils.func_tools import func_cache
from deal_pilot.utils.sandbox import enter_shared_sandbox

# 共享卷上 skill 包解压目录（用户沙箱 /shared 只读可见）
SHARED_SKILLS_ROOT = "/shared/skills"


def _validate_path_segment(name: str, *, field: str) -> str:
    segment = name.strip().strip("/")
    if not segment or "/" in segment or ".." in segment:
        raise ValueError(f"无效的 {field}: {name}")
    return segment


def agent_skills_root(agent_name: str) -> str:
    """某 Agent 在共享卷上的 skill 根目录，如 /shared/skills/main_agent。"""
    return f"{SHARED_SKILLS_ROOT}/{_validate_path_segment(agent_name, field='agent_name')}"


def skill_dir(agent_name: str, skill_name: str) -> str:
    return f"{agent_skills_root(agent_name)}/{_validate_path_segment(skill_name, field='skill_name')}"


def _unpack_skill_zip(content: bytes) -> dict[str, bytes]:
    """解压 zip，去掉唯一的顶层目录（如 pptx/...）。"""
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        files: dict[str, bytes] = {}
        for info in zf.infolist():
            if info.is_dir():
                continue
            path = PurePosixPath(info.filename)
            if path.parts and path.parts[0] == "__MACOSX":
                continue
            files[str(path)] = zf.read(info.filename)
    if not files:
        return {}
    roots = {PurePosixPath(p).parts[0] for p in files if PurePosixPath(p).parts}
    if len(roots) == 1:
        root = next(iter(roots))
        if all(
            len(PurePosixPath(p).parts) > 1 and PurePosixPath(p).parts[0] == root
            for p in files
        ):
            files = {
                str(PurePosixPath(*PurePosixPath(k).parts[1:])): v
                for k, v in files.items()
            }
    return files


@dataclass(frozen=True)
class SkillPackage:
    name: str
    content: bytes


@func_cache(ttl=nacos_settings.ttl, lazy=True)
async def get_skill(skill_name: str, *, label: str = "production") -> SkillPackage | None:
    access_token = await get_access_token()
    async with httpx.AsyncClient(
        base_url=nacos_settings.server_address,
        headers={"accessToken": access_token},
    ) as client:
        response = await client.get(
            "/nacos/v3/client/ai/skills",
            params={
                "namespaceId": nacos_settings.namespace_id,
                "name": skill_name,
                "label": label,
            },
        )

    if response.status_code in (404, 304):
        return None

    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        body = response.json()
        if body.get("code") != 0:
            return None
        return None

    response.raise_for_status()
    return SkillPackage(name=skill_name, content=response.content)


async def get_skills(
    skill_names: list[str],
    *,
    label: str = "production",
) -> dict[str, SkillPackage]:
    tasks = [get_skill(name, label=label) for name in skill_names]
    packages = await asyncio.gather(*tasks)
    return {package.name: package for package in packages if package is not None}


async def _write_skill_tree(
    sandbox, dest_dir: str, files: dict[str, bytes]
) -> None:
    await sandbox.commands.run(f"mkdir -p {dest_dir}")
    for rel_path in sorted(files, key=lambda p: (p.count("/"), p)):
        full_path = f"{dest_dir}/{rel_path}"
        parent = str(PurePosixPath(full_path).parent)
        if parent != dest_dir.rstrip("/") and parent != dest_dir:
            await sandbox.commands.run(f"mkdir -p {parent}")
        await sandbox.files.write_file(full_path, files[rel_path])


async def save_skills(
    skill_names: list[str],
    agent_name: str,
    *,
    label: str = "production",
) -> None:
    """从 Nacos 拉取 skill 包，经共享沙箱写入共享卷。"""
    if not skill_names:
        return
    skills_root = agent_skills_root(agent_name)
    packages = await get_skills(skill_names, label=label)
    if not packages:
        return
    async with enter_shared_sandbox() as sandbox:
        await sandbox.commands.run(f"mkdir -p {skills_root}")
        for name, package in packages.items():
            dest_dir = skill_dir(agent_name, name)
            await sandbox.commands.run(f"rm -rf -- {dest_dir}")
            content = package.content
            if content[:2] == b"PK":
                files = _unpack_skill_zip(content)
                if not files:
                    continue
                await _write_skill_tree(sandbox, dest_dir, files)
            else:
                await sandbox.commands.run(f"mkdir -p {dest_dir}")
                await sandbox.files.write_file(f"{dest_dir}/SKILL.md", content)


if __name__ == "__main__":

    async def main():
        result = await get_skill("pptx")
        print(result, len(result.content) if result else None)

    asyncio.run(main())
