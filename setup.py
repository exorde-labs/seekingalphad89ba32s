from setuptools import find_packages, setup

setup(
    name="seekingalphad89ba32s",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "exorde_data",
        "aiohttp",
        "beautifulsoup4>=4.11"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
