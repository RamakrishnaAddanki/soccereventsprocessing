from setuptools import setup, find_packages

setup(name='pyspark_ci_demo',
      version='0.1',
      description='PySpark application',
      author='ramakrishna.addanki',
      author_email='',
      zip_safe=False,
      license="",
      packages=find_packages(),
      py_modules=['__main__'], install_requires=['Py4j', 'pyspark', 'pytest']
      )
