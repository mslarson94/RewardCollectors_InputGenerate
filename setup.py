sfrom setuptools import setup, find_packages

setup(
    name="RC_utilities",  # Give your package a unique name
    version="1.1",
    packages=find_packages(),  # Automatically find all packages, including `helper_functions`
    install_requires=[],  # Add dependencies here if needed
    description="Config files and helper functions for RewardCollectors project",
    author="Myra Sarai Larson",
    author_email="mairahsarai94@gmail.com",
)
