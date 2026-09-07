```ini
[app]

# Application name
title = My Online Game

# Package name
package.name = myonlinegame

# Package domain
package.domain = org.mygame

# Main source directory
source.dir = .

# Main Python file
source.include_exts = py,png,jpg,kv,atlas

# Application version
version = 1.0

# Python requirements
requirements = python3,kivy

# Android permissions
android.permissions = INTERNET,ACCESS_NETWORK_STATE

# Orientation
orientation = portrait

# Fullscreen
fullscreen = 0


[buildozer]

# Log level
log_level = 2

# Warning level
warn_on_root = 1


[app:android]

# Android API
android.api = 35

# Minimum Android API
android.minapi = 23

# Architecture
android.archs = arm64-v8a

# Android backup
android.allow_backup = True
