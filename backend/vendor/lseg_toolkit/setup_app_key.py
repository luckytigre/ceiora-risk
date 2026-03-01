#!/usr/bin/env python3
"""
Interactive setup script for configuring LSEG app key.

Creates config file with user-provided app key at:
- Global: ~/.lseg/config.json (default)
- Local: .lseg-config.json (project-specific)
"""

import json
import sys
from pathlib import Path


def main():
    """Interactive setup for LSEG app key configuration."""
    print("=" * 70)
    print("LSEG App Key Setup")
    print("=" * 70)
    print()
    print("This script will help you configure your LSEG API app key.")
    print()
    print("To generate an app key:")
    print("1. Open LSEG Workspace Desktop")
    print("2. Navigate to Settings/Preferences")
    print("3. Find 'API' or 'App Key' section")
    print("4. Generate a new app key")
    print()

    # Get app key from user
    app_key = input("Enter your LSEG app key (or press Enter to cancel): ").strip()

    if not app_key:
        print("\nSetup cancelled.")
        sys.exit(0)

    # Validate app key format (basic check - alphanumeric and reasonable length)
    if len(app_key) < 20 or not all(c.isalnum() for c in app_key):
        print("\nWarning: App key format looks unusual.")
        print("   App keys are typically 40+ character alphanumeric strings.")
        confirm = input("   Continue anyway? [y/N]: ").strip().lower()
        if confirm != "y":
            print("\nSetup cancelled.")
            sys.exit(0)

    print()
    print("Where would you like to store the config?")
    print()
    print("1. Global (~/.lseg/config.json)")
    print("   - Used by all projects")
    print("   - Recommended for most users")
    print()
    print("2. Local (.lseg-config.json in current directory)")
    print("   - Only used by this project")
    print("   - Useful for project-specific keys")
    print()

    choice = input("Choose location [1/2, default=1]: ").strip() or "1"

    if choice == "1":
        config_path = Path.home() / ".lseg" / "config.json"
        location_name = "global"
    elif choice == "2":
        config_path = Path.cwd() / ".lseg-config.json"
        location_name = "local"
    else:
        print(f"\nError: Invalid choice: {choice}")
        sys.exit(1)

    # Create config data
    config_data = {"app_key": app_key}

    # Create directory if needed (for global config)
    if not config_path.parent.exists():
        config_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if config already exists
    if config_path.exists():
        print(f"\nWarning: Config file already exists: {config_path}")
        overwrite = input("   Overwrite? [y/N]: ").strip().lower()
        if overwrite != "y":
            print("\nSetup cancelled.")
            sys.exit(0)

    # Write config file
    try:
        with open(config_path, "w") as f:
            json.dump(config_data, f, indent=2)

        print()
        print("=" * 70)
        print(f"Success! App key configured ({location_name})")
        print("=" * 70)
        print(f"\nConfig file created: {config_path}")
        print()
        print("Your LSEG toolkit will now use this app key automatically.")
        print()

        if location_name == "local":
            print("IMPORTANT: Add .lseg-config.json to your .gitignore file!")
            print("   Do NOT commit your app key to version control.")
            print()

    except Exception as e:
        print(f"\nError writing config file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nSetup cancelled.")
        sys.exit(0)
