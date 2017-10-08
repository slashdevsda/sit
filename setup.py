from setuptools import setup

setup(name='sit',
      version='0.1',
      description='cli import / export database tool',
      url='http://example.com',
      author='Thomas E',
      author_email='fixme@example.com',
      license='MIT',
      packages=['sit'],
      install_requires=[
          'pymssql',
          'prompt_toolkit',
          'pygments'
      ],
      scripts=['bin/sit'],
      zip_safe=False)
