open:
    -explorer.exe "$(wslpath -w {{justfile_directory()}}/pluck_gantt.html)"

view logfile='pluck.log':
    python3 {{justfile_directory()}}/log_gantt.py {{justfile_directory()}}/{{logfile}} -o {{justfile_directory()}}/pluck_gantt.html
    -explorer.exe "$(wslpath -w {{justfile_directory()}}/pluck_gantt.html)"
