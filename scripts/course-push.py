#!/usr/bin/env python3
"""交互式变基并强制推送到服务器。"""

import os
import subprocess
import sys

from rich.console import Console
from rich.panel import Panel

console = Console()


def is_rebase_in_progress() -> bool:
    r = subprocess.run(
        ["git", "rev-parse", "--git-path", "rebase-merge"],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return r.returncode == 0 and os.path.exists(r.stdout.strip())


def main() -> None:
    if len(sys.argv) < 2:
        console.print(
            Panel(
                "[bold red]请提供要开始修改的提交 hash[/]\n"
                "[dim]用法: python scripts/course-push.py <commit>[/]",
                border_style="red",
            )
        )
        sys.exit(1)

    commit = sys.argv[1]

    check = subprocess.run(
        ["git", "cat-file", "-t", commit],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if check.returncode != 0:
        console.print(
            Panel(
                f"[bold red]找不到提交: {commit}[/]",
                border_style="red",
            )
        )
        sys.exit(1)

    console.print(
        Panel(
            f"[bold]即将从 [cyan]{commit}[/] 开始交互式变基[/]",
            border_style="cyan",
        )
    )
    console.print()
    console.print("[dim]1. 要修改哪个提交，就把那行的 pick 改为 edit，保存退出[/]")
    console.print("[dim]2. 变基停顿后，在编辑器中修改代码文件[/]")
    console.print("[dim]3. 修改完成后回来按 Enter，脚本自动 amend 并继续[/]")
    console.print("[dim]4. 全部完成后自动推送到服务器[/]")
    console.print()

    input("按 Enter 打开编辑器开始变基…")

    subprocess.run(["git", "rebase", "-i", f"{commit}^"])

    while is_rebase_in_progress():
        console.print()
        console.print("[bold yellow]变基已暂停[/]")
        console.print("[dim]请在编辑器中修改代码文件，完成后回到这里按 Enter[/]")
        console.print()
        input("按 Enter 自动 commit 并继续…")

        subprocess.run(["git", "add", "."], capture_output=True, encoding="utf-8")
        subprocess.run(
            ["git", "commit", "--amend", "--no-edit"],
            capture_output=True,
            encoding="utf-8",
        )
        console.print("[dim]正在继续变基…[/]")
        subprocess.run(["git", "rebase", "--continue"])

    console.print()
    console.print("[dim]变基完成，正在推送到服务器…[/]")

    push = subprocess.run(
        ["git", "push", "--force-with-lease", "origin", "main"],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    if push.returncode == 0:
        console.print(
            Panel(
                "[bold green]已强制推送到 origin/main[/]",
                title=":white_check_mark:  推送完成",
                border_style="green",
            )
        )
    else:
        console.print(
            Panel(
                f"[bold red]推送失败[/]\n[dim]{push.stderr.strip()}[/]",
                border_style="red",
            )
        )


if __name__ == "__main__":
    main()
