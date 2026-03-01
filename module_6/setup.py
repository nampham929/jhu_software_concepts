from setuptools import find_packages, setup


setup(
    name="module_6",
    version="0.1.0",
    description="GradCafe analytics dashboard",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        "Flask==3.1.3",
        "python-dotenv==1.1.1",
        "psycopg[binary]==3.3.2",
        "pika==1.3.2",
        "beautifulsoup4==4.14.3",
        "Werkzeug==3.1.6",
    ],
    python_requires=">=3.10",
)
