import re
import os

def main():
    with open("run.py", "r") as f:
        content = f.read()

    start_marker = '@flask_app.cli.command("seed-markets")'
    end_marker = 'if __name__ == "__main__":'

    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker)
    
    if start_idx == -1 or end_idx == -1:
        print("Markers not found!")
        return

    header = content[:start_idx]
    cli_body = content[start_idx:end_idx]
    footer = content[end_idx:]

    # Instead of making 1 giant file, let's create a Blueprint in app/cli/__init__.py
    # and split the body into logical files.
    
    os.makedirs("app/cli", exist_ok=True)
    
    # Let's replace @flask_app.cli.command with @bp.cli.command
    cli_body = cli_body.replace("@flask_app.cli.command", "@bp.cli.command")
    
    # Wait, flask_app is used in one place for register_harvest_view
    cli_body = cli_body.replace("def register_harvest_view(app):", "def register_harvest_view(app=None):")
    cli_body = cli_body.replace("@app.route", "@bp.route")
    
    # We will write the entire CLI body to app/cli/commands.py first.
    # Then import it in app/cli/__init__.py
    
    commands_py = f"""import os
import sys
import json
import click
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from flask import Blueprint, current_app, request, render_template_string
from app.extensions import db

bp = Blueprint("cli_commands", __name__, cli_group=None)

{cli_body}

def register_cli_commands(app):
    app.register_blueprint(bp)
    register_harvest_view() # Register the view on the blueprint
"""

    with open("app/cli/commands.py", "w") as f:
        f.write(commands_py)
        
    with open("app/cli/__init__.py", "w") as f:
        f.write("from .commands import register_cli_commands\n")

    new_run_py = header + "\nfrom app.cli import register_cli_commands\nregister_cli_commands(flask_app)\n\n" + footer
    with open("run.py", "w") as f:
        f.write(new_run_py)
        
    print("Extraction complete. All commands moved to app/cli/commands.py")

if __name__ == "__main__":
    main()
