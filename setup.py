from setuptools import setup
setup(
    name="treepush",
    version="0.1",
    author='JinXing',
    author_email='jxinging@gmail.com',
    url='https://github.com/jinxingxing/treepush',
    packages=['treepush'],
    install_requires=[],
    entry_points="""
    [console_scripts]
    treepush = treepush.treepush:main
    """
)
