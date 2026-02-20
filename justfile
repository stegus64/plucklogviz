default:
    @just --list

open:
    -explorer.exe "$(wslpath -w {{justfile_directory()}}/pluck_gantt.html)"

view logfile='pluck.log':
    python3 {{justfile_directory()}}/log_gantt.py {{justfile_directory()}}/{{logfile}} -o {{justfile_directory()}}/pluck_gantt.html
    -explorer.exe "$(wslpath -w {{justfile_directory()}}/pluck_gantt.html)"

view-prod:
    powershell.exe -NoProfile -Command "Copy-Item -LiteralPath '\\\\bi-sql-tst\\c$\\bidev01\\pluck\\pluck.log' -Destination '$(wslpath -w {{justfile_directory()}}/pluck.prod.log)' -Force"
    python3 {{justfile_directory()}}/log_gantt.py {{justfile_directory()}}/pluck.prod.log -o {{justfile_directory()}}/pluck_gantt.html
    -explorer.exe "$(wslpath -w {{justfile_directory()}}/pluck_gantt.html)"
