#!/usr/bin/env python3
"""切换课程，并将本节课相对上一节课的变化留在工作区。"""

import pathlib
import re
import subprocess
import sys

import questionary
from questionary import Style
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()
SCRIPT_PATH = "scripts/course.py"

CUSTOM_STYLE = Style(
    [
        ("qmark", "fg:#ff5f87 bold"),
        ("question", "fg:#e0e0e0 bold"),
        ("answer", "fg:#5f87ff bold"),
        ("pointer", "fg:#ff5f00 bold"),
        ("highlighted", "fg:#ffffff bg:#5f87ff bold"),
        ("selected", "fg:#5f87ff"),
        ("separator", "fg:#444444"),
        ("instruction", "fg:#5fafd7 italic"),
    ]
)


def get_commits() -> list[dict]:
    result = subprocess.run(
        ["git", "log", "main", "--format=%H;%s"],
        capture_output=True,
        text=True,
        check=True,
        encoding="utf-8",
        errors="replace",
    )
    commits: list[dict] = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split(";", 1)
        if len(parts) != 2:
            continue
        m = re.match(r"^(\d+)\.\s*(.*)", parts[1])
        if m:
            commits.append(
                {
                    "hash": parts[0],
                    "number": m.group(1),
                    "message": m.group(2),
                }
            )
    return commits


def get_current_head() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return result.stdout.strip()


def get_current_branch() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return result.stdout.strip()


def git_output(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    ).stdout.rstrip("\n")


def git_bytes(*args: str) -> bytes:
    return subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
    ).stdout


def course_base(commits: list[dict], number: str) -> tuple[str | None, str] | None:
    """上一节课的提交；第一节课以它的父提交为基线。"""
    for index, commit in enumerate(commits):
        if commit["number"] == number:
            if index + 1 < len(commits):
                previous = commits[index + 1]["hash"]
            else:
                parent = subprocess.run(
                    ["git", "rev-parse", "--verify", f"{commit['hash']}^"],
                    capture_output=True,
                    text=True,
                )
                previous = parent.stdout.strip() if parent.returncode == 0 else None
            return previous, commit["hash"]
    return None


def current_course(commits: list[dict]) -> str | None:
    branch = get_current_branch()
    pair = course_base(commits, branch)
    return branch if pair and get_current_head() in pair else None


def prepare_worktree() -> None:
    """切课前丢弃当前的暂存、未暂存和未跟踪修改。"""
    if (
        subprocess.run(
            ["git", "ls-files", "--error-unmatch", "--", SCRIPT_PATH],
            capture_output=True,
        ).returncode
        == 0
    ):
        subprocess.run(
            ["git", "update-index", "--no-assume-unchanged", "--", SCRIPT_PATH],
            check=True,
        )
    if git_output("status", "--porcelain", "--untracked-files=normal"):
        console.print("[yellow]正在清理当前未提交的修改…[/]")
    subprocess.run(["git", "reset", "--hard", "HEAD"], check=True, capture_output=True)
    subprocess.run(["git", "clean", "-fd"], check=True, capture_output=True)


def keep_current_script() -> None:
    """早期课程提交中的脚本是旧版，保留 main 上的切课能力。"""
    pathlib.Path(SCRIPT_PATH).write_bytes(git_bytes("show", f"main:{SCRIPT_PATH}"))
    subprocess.run(
        ["git", "update-index", "--assume-unchanged", "--", SCRIPT_PATH], check=True
    )


def checkout(number: str, target: str, previous: str | None) -> None:
    existing = (
        subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{number}"],
        ).returncode
        == 0
    )
    if existing:
        branch_head = git_output("rev-parse", number)
        if branch_head not in {previous, target}:
            raise RuntimeError(f"分支 {number} 含有自己的提交，请先处理该分支")
        subprocess.run(["git", "switch", number], check=True)
        if previous and branch_head == target:
            subprocess.run(["git", "reset", "--mixed", previous], check=True)
        elif previous:
            subprocess.run(
                ["git", "restore", "--source", target, "--worktree", "--", "."],
                check=True,
            )
    else:
        subprocess.run(["git", "switch", "-c", number, target], check=True)
        if previous:
            subprocess.run(["git", "reset", "--mixed", previous], check=True)

    if previous:
        added = (
            git_bytes(
                "diff", "--name-only", "--diff-filter=A", "-z", previous, target, "--"
            )
            .decode()
            .split("\0")
        )
        added = [path for path in added if path]
        if added:
            subprocess.run(["git", "add", "-N", "-f", "--", *added], check=True)
    keep_current_script()


def switch_course() -> None:
    commits = get_commits()

    if not commits:
        console.print(
            Panel(
                "[bold]没有找到 message 格式为「数字. 消息」的提交[/]",
                title=":warning:  提示",
                border_style="red",
            )
        )
        return

    current_head = get_current_head()
    current_number = current_course(commits) or next(
        (c["number"] for c in commits if c["hash"] == current_head), None
    )

    header = Text()
    header.append("课程分支切换\n", style="bold white")
    header.append(f"共发现 ", style="dim")
    header.append(f"{len(commits)}", style="bold cyan")
    header.append(f" 个课程提交", style="dim")
    if current_number:
        header.append(f"  |  当前: ", style="dim")
        header.append(current_number, style="bold green")
    console.print(Panel(header, border_style="cyan"))
    console.print()

    choices = [
        questionary.Choice(
            title=(
                f"{'●' if c['number'] == current_number else ' '} "
                f"{c['number']:<6} {c['message'][:56]}{'…' if len(c['message']) > 56 else ''}"
            ),
            value=c,
        )
        for c in commits
    ]

    selected = questionary.select(
        "请选择要切换到的课程提交",
        choices=choices,
        style=CUSTOM_STYLE,
        use_shortcuts=False,
    ).ask()

    if selected is None:
        console.print("\n[dim]已取消[/]")
        return

    pair = course_base(commits, selected["number"])
    assert pair is not None
    previous, target = pair
    try:
        prepare_worktree()
        checkout(selected["number"], target, previous)
    except (RuntimeError, subprocess.CalledProcessError) as exc:
        console.print(
            Panel(f"[bold red]切换失败[/]\n[dim]{exc}[/]", border_style="red")
        )
        return
    console.print()
    console.print(
        Panel(
            f"[bold green]{selected['number']}[/]\n[dim]{selected['message']}[/]"
            + (
                "\n[cyan]第一节课相对初始提交的变化已留在未提交状态[/]"
                if selected["number"] == commits[-1]["number"] and previous
                else (
                    "\n[cyan]本节课相对上一节课的变化已留在未提交状态[/]"
                    if previous
                    else "\n[dim]首个提交没有可对比的基线[/]"
                )
            ),
            title=":white_check_mark:  切换成功",
            border_style="green",
        )
    )


def switch_main() -> None:
    try:
        prepare_worktree()
        subprocess.run(["git", "switch", "main"], check=True)
    except (RuntimeError, subprocess.CalledProcessError) as exc:
        console.print(
            Panel(f"[bold red]切换失败[/]\n[dim]{exc}[/]", border_style="red")
        )
        return
    console.print(Panel("[bold green]已切换到 main[/]", border_style="green"))


def update_courses() -> None:
    console.print(Panel("更新资料", style="bold white", border_style="cyan"))
    console.print()

    # 1. fetch
    console.print("[dim]正在从服务器拉取最新数据…[/]")
    fetch_result = subprocess.run(
        ["git", "fetch", "origin"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if fetch_result.returncode != 0:
        console.print(
            Panel(
                f"[bold red]拉取失败[/]\n[dim]{fetch_result.stderr.strip()}[/]",
                border_style="red",
            )
        )
        return

    # 2. 检查 origin/main 是否存在
    origin_rev = subprocess.run(
        ["git", "rev-parse", "origin/main"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if origin_rev.returncode != 0:
        console.print(
            Panel(
                "[bold red]远程分支 origin/main 不存在[/]",
                border_style="red",
            )
        )
        return
    origin_head = origin_rev.stdout.strip()

    # 3. 检查本地 main 是否存在
    local_rev = subprocess.run(
        ["git", "rev-parse", "main"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    local_main_exists = local_rev.returncode == 0
    local_head = local_rev.stdout.strip() if local_main_exists else None

    # 4. 已是最新
    if local_main_exists and local_head == origin_head:
        console.print(
            Panel(
                "[green]本地 main 已与服务器一致，无需更新[/]",
                border_style="green",
            )
        )
        return

    # 5. 判断能否快进
    current_branch = get_current_branch()

    if local_main_exists:
        ff_check = subprocess.run(
            ["git", "merge-base", "--is-ancestor", "main", "origin/main"],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
        can_ff = ff_check.returncode == 0
    else:
        can_ff = False

    if can_ff:
        console.print("[dim]服务器有新提交，执行快进…[/]")
        if current_branch != "main":
            prepare_worktree()
            subprocess.run(
                ["git", "checkout", "main"],
                check=True,
                capture_output=True,
                encoding="utf-8",
                errors="replace",
            )
        subprocess.run(
            ["git", "merge", "--ff-only", "origin/main"],
            check=True,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
        console.print(
            Panel(
                "[bold green]main 已快进到最新[/]",
                title=":white_check_mark:  更新完成",
                border_style="green",
            )
        )
        return

    # 6. 无法快进 —— 强制同步

    console.print("[dim]分支历史不一致，正在强制同步…[/]")
    prepare_worktree()

    # 7. 执行强制更新
    # 先 detach，以免当前分支被删除时报错
    subprocess.run(
        ["git", "checkout", "--detach", "--force", "origin/main"],
        check=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )

    # 删除除 main 外的所有本地分支
    branches_result = subprocess.run(
        ["git", "for-each-ref", "refs/heads/", "--format=%(refname:short)"],
        capture_output=True,
        text=True,
        check=True,
        encoding="utf-8",
        errors="replace",
    )
    for branch in branches_result.stdout.strip().split("\n"):
        if branch and branch != "main":
            subprocess.run(
                ["git", "branch", "-D", branch],
                check=True,
                capture_output=True,
                encoding="utf-8",
                errors="replace",
            )

    # 强制将 main 指向 origin/main 并切过去
    subprocess.run(
        ["git", "checkout", "-B", "main", "origin/main"],
        check=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )

    console.print(
        Panel(
            "[bold green]main 已强制同步到服务器版本，其余分支已全部删除[/]",
            title=":white_check_mark:  强制更新完成",
            border_style="green",
        )
    )


def main() -> None:
    action = questionary.select(
        "请选择操作",
        instruction="\n(你可以在任何情况下运行该脚本，不需要手动做任何处理，如果遇到问题，请告知班主任复现问题的流程，方便我修复)",
        choices=[
            questionary.Choice("切换资料", value="switch"),
            questionary.Choice("切回 main", value="main"),
            questionary.Choice("更新资料", value="update"),
        ],
        style=CUSTOM_STYLE,
    ).ask()

    if action is None:
        console.print("\n[dim]已取消[/]")
        sys.exit(0)
    elif action == "switch":
        switch_course()
    elif action == "main":
        switch_main()
    elif action == "update":
        update_courses()


if __name__ == "__main__":
    main()
