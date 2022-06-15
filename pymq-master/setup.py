from setuptools import setup, find_packages

setup(name="pymq",
      version="1.0.0",
      description="Python MQ client",
      url="https://gitlab.com/grtid/pymq",
      author="Ivan Lozitsky",
      packages=find_packages(),
      install_requires=["amqpstorm"]
      )
