from setuptools import find_packages, setup

requirements = ["typer[all]==0.9.0", "rich==13.4.2", "textual==0.32.0"]

setup(
    name="slurmtui",
    version="0.0.1",
    packages=find_packages("src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": [
            "slurmtui = slurmtui.main:entry_point",
            "slurmui = slurmtui.main:entry_point",
            "sui = slurmtui.main:entry_point",
        ],
    },
    install_requires=requirements,
    package_data={
        "slurmtui": ["css/*"],
    },
)
