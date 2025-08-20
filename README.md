# SlurmTUI
[![PyPI version](https://badge.fury.io/py/slurmtui.svg)](https://badge.fury.io/py/slurmtui)
![PyPI - Downloads](https://img.shields.io/pypi/dm/slurmtui)

A simple Terminal UI for monitoring SLURM jobs.

### Note: SlurmTUI requires slurm 21.08 or later for the Json output support.

## Installation
```bash
pip install slurmtui
```

## Usage
Just run `slurmtui`/`slurmui`/`sui` in your terminal.

Settings can be configured in `~/.slurmtui_settings.json`, you can override the settings path by setting `SLURMTUI_SETTINGS`.

Settings can also be overridden by passing arguments to the command line, for example:
```bash
slurmtui --update-interval 5
```

All extra arguments will be passed to `squeue` command, for example:
```bash
slurmtui --account my_account
```


![jobui](./img/screenshot.png)

# List of planned features (not in the priority order):

- [x] View old jobs
- [x] Filtering jobs when launching
- [ ] Sorting
- [ ] Options other than tail for logs
- [x] Faster launch
- [x] Remove Array columns if no job array exists
- [ ] Search
- [ ] Display used/available resources

# FAQ

### How to select text in the App?
JobUI is running a Textual app which puts your terminal in to application mode which disables clicking and dragging to select text. Most terminal emulators offer a modifier key which you can hold while you click and drag to restore the behavior you may expect from the command line. The exact modifier key depends on the terminal and platform you are running on.

- iTerm Hold the OPTION key.
- Gnome Terminal Hold the SHIFT key.
- Windows Terminal Hold the SHIFT key.

Refer to the documentation for your terminal emulator, if it is not listed above.

# Contact
- [Wissam Antoun](https://github.com/WissamAntoun/)