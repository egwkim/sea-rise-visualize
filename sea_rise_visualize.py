import json
import os
import urllib.request
import urllib.parse
import zipfile
from multiprocessing.pool import Pool

import geopandas
import rasterio
from rasterio import features, plot

BUFFER_SIZE = 256*1024

DATA_DIR = './data'


def get_filename_url(url: str) -> str:
    return url.split('/')[-1].split('?')[0]


def download_data() -> None:
    try:
        os.mkdir(DATA_DIR)
    except FileExistsError:
        pass

    print('Downloading data... This might take a while.')
    with Pool() as pool:
        # Blue marble
        try:
            os.mkdir(os.path.join(DATA_DIR, 'blue-marble'))
            urls = ['https://eoimages.gsfc.nasa.gov/images/imagerecords/57000/57752/land_shallow_topo_2048.jpg',
                    'https://eoimages.gsfc.nasa.gov/images/imagerecords/57000/57752/land_shallow_topo_8192.tif',
                    'https://eoimages.gsfc.nasa.gov/images/imagerecords/73000/73938/world.200401.3x5400x2700.png',
                    'https://eoimages.gsfc.nasa.gov/images/imagerecords/74000/74167/world.200410.3x5400x2700.png']
            for url in urls:
                pool.apply_async(download_file,
                                (url, get_filename_url(url), 'blue-marble'))
        except FileExistsError:
            pass

        # ETOPO
        try:
            os.mkdir(os.path.join(DATA_DIR, 'ETOPO'))
            urls = ['https://www.ngdc.noaa.gov/mgg/global/relief/ETOPO2022/data/60s/60s_bed_elev_gtif/ETOPO_2022_v1_60s_N90W180_bed.tif',
                    'https://www.ngdc.noaa.gov/mgg/global/relief/ETOPO2022/data/60s/60s_geoid_gtif/ETOPO_2022_v1_60s_N90W180_geoid.tif',
                    'https://www.ngdc.noaa.gov/mgg/global/relief/ETOPO2022/data/60s/60s_surface_elev_gtif/ETOPO_2022_v1_60s_N90W180_surface.tif', ]
            for url in urls:
                pool.apply_async(
                    download_file, (url, get_filename_url(url), 'ETOPO'))
            # Korean peninsula with higher resolution
            pool.apply_async(download_file,
                            ('https://gis.ngdc.noaa.gov/arcgis/rest/services/DEM_mosaics/DEM_all/ImageServer/exportImage?bbox=124.00000,33.00000,134.00000,43.00000&bboxSR=4326&size=2400,2400&imageSR=4326&format=tiff&pixelType=F32&interpolation=+RSP_NearestNeighbor&compression=LZ77&renderingRule={%22rasterFunction%22:%22none%22}&mosaicRule={%22where%22:%22Name=%27ETOPO_2022_v1_15s_bed_elev%27%22}&f=image',
                            'ETOPO_2022_v1_15s_N43W124_bed.tiff', 'ETOPO'))
        except FileExistsError:
            pass

        # GSHHG
        if not os.path.exists(os.path.join(DATA_DIR, 'gshhg-shp-2.3.7')):
            url = 'https://www.ngdc.noaa.gov/mgg/shorelines/data/gshhg/latest/gshhg-shp-2.3.7.zip'
            pool.apply_async(download_file, (url, get_filename_url(url)))

        # Natural earth land
        try:
            os.mkdir(os.path.join(DATA_DIR, 'ne_10m_land'))
            files: list = json.load(
                urllib.request.urlopen('https://api.github.com/repos/nvkelso/natural-earth-vector/contents/10m_physical'))
            files = list(
                filter(lambda x: x['name'].startswith('ne_10m_land.'), files))
            for file in files:
                pool.apply_async(
                    download_file, (file['download_url'], file['name'], 'ne_10m_land'))
        except FileExistsError:
            pass

        try:
            os.mkdir(os.path.join(DATA_DIR, 'ne_110m_land'))
            files: list = json.load(
                urllib.request.urlopen('https://api.github.com/repos/nvkelso/natural-earth-vector/contents/110m_physical'))
            files = list(
                filter(lambda x: x['name'].startswith('ne_110m_land.'), files))
            for file in files:
                pool.apply_async(
                    download_file, (file['download_url'], file['name'], 'ne_110m_land'))
        except FileExistsError:
            pass

        pool.close()
        pool.join()


def download_file(url: str, filename: str, dir: str = '') -> None:
    filepath = os.path.join(DATA_DIR, dir, filename)
    urllib.request.urlretrieve(url, filepath)
    print(filepath)
    if filepath.split('.')[-1] == 'zip':
        with zipfile.ZipFile(filepath, 'r') as f:
            f.extractall('.'.join(filepath.split('.')[:-1]))
        os.remove(filepath)


def main() -> None:
    try:
        download_data()
    except FileExistsError as e:
        # Data directory already exists
        pass


if __name__ == '__main__':
    main()
