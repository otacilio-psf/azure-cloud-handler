import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='azurecloudhandler',  
     version='0.2',
     description="Library to optimally handle some resources on Azure",
     url="https://github.com/otacilio-psf/azure-cloud-handler",
     author="Otacilio Filho",
     author_email="otaciliopedro@gmail.com",
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
     packages=setuptools.find_packages(),
     install_requires=[
         'azure-storage-file-datalake>=12.5.0',
         'requests>=2.22.0',
         'azure-identity>=1.7.1'
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
 )
