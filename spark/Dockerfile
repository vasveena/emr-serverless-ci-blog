FROM public.ecr.aws/emr-serverless/spark3/emr-6.9.0:latest

USER root

# Install US census data to the image 
ADD https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_state_500k.zip  /usr/local/share/bokeh/
ADD https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip /usr/local/share/bokeh/
RUN chmod 644 /usr/local/share/bokeh/cb*.zip

# Install Chrome
RUN curl https://intoli.com/install-google-chrome.sh | bash && \
    mv /usr/bin/google-chrome-stable /usr/bin/chrome

# We need to upgrade pip in order to install pyproj
RUN pip3 install --upgrade pip

# If you pip install as root, use this
RUN pip3 install \
    bokeh==2.3.2 \
    boto3==1.17.93 \
    chromedriver-py==108.0.5359.71 \
    geopandas==0.9.0 \
    selenium==3.141.0 \
    shapely==1.7.1

RUN ln -s /usr/local/lib/python3.7/site-packages/chromedriver_py/chromedriver_linux64 /usr/local/bin/chromedriver

# Install bokeh sample data to a tmpdir
RUN mkdir /root/.bokeh && \
    echo "sampledata_dir: /usr/local/share/bokeh" > /root/.bokeh/config

RUN bokeh sampledata

USER hadoop:hadoop
