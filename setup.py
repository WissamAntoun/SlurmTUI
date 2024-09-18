from setuptools import find_packages, setup

requirements = ["typer[all]==0.9.0", "rich==13.4.2", "textual==0.32.0"]


def get_long_description():
    with open("README.md", "r", encoding="utf-8") as f:
        long_description = f.read()

    return long_description


setup(
    name="slurmtui",
    version="0.2.2",
    author="Wissam Antoun",
    author_email="wissam.antoun@gmail.com",
    description="A simple Terminal UI (TUI) for Slurm",
    long_description=get_long_description().replace(
        "./img/screenshot.png",
        "https://raw.githubusercontent.com/WissamAntoun/SlurmTUI/main/img/screenshot.png",
    ),
    url="https://github.com/WissamAntoun/SlurmTUI",
    long_description_content_type="text/markdown",
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
    python_requires=">=3.6.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
    ],
)
