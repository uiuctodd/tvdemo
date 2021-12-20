from setuptools import setup

setup(
   name='python-todd-tvdemo',
   version='0.1.0',
   author='Todd Markle',
   author_email='todd@eclexia.com',
   packages=['tvdemo'],
   install_requires=[
       'MySQLdb',
       'mysql.connector',
       'pandas',
       'urllib',
       'configparser',
     ],
   );


