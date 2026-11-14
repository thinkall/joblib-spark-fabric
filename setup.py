from pathlib import Path
from runpy import run_path

from setuptools import setup, find_packages

if __name__ == '__main__':
    package_info = run_path(str(Path(__file__).parent / 'joblibspark' / '__init__.py'))
    setup(name='joblibspark-fabric',
          version=package_info['__version__'],
          author='Weichen Xu',
          author_email='weichen.xu@databricks.com',
          maintainer='joblibspark-fabric maintainers',
          url='https://github.com/thinkall/joblib-spark-fabric',
          project_urls={
              'Source': 'https://github.com/thinkall/joblib-spark-fabric',
              'Changelog': 'https://github.com/thinkall/joblib-spark-fabric/blob/main/CHANGES.rst',
          },
          description="Independently maintained Apache Spark backend for joblib",
          long_description=Path(__file__).with_name('README.md').read_text(encoding='utf-8'),
          long_description_content_type='text/markdown',
          license='Apache-2.0',
          license_files=['LICENSE'],
          classifiers=[
              'Development Status :: 5 - Production/Stable',
              'Environment :: Console',
              'Intended Audience :: Developers',
              'Intended Audience :: Science/Research',
              'Intended Audience :: Education',
              'Operating System :: OS Independent',
              'Programming Language :: Python :: 3',
              'Programming Language :: Python :: 3.13',
              'Programming Language :: Python :: 3.14',
              'Topic :: Scientific/Engineering',
              'Topic :: Utilities',
              'Topic :: Software Development :: Libraries',
          ],
          platforms='any',
          packages=find_packages(include=['joblibspark', 'joblibspark.*']),
          python_requires='>=3.13,<3.15',
          install_requires=[
              'joblib>=1.5.0',
              'packaging',
          ],
          extras_require={
              'spark': ['pyspark>=4.0,<=4.3.0'],
              'connect': ['pyspark[connect]>=4.0,<=4.3.0'],
              'test': ['pytest', 'pylint'],
          })
