import json
import os
import urllib.request
import urllib.parse
import zipfile
from multiprocessing.pool import Pool

import numpy as np
import matplotlib.colors
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import matplotlib.font_manager as fm
import rasterio

BUFFER_SIZE = 256*1024

DATA_DIR = './data'


class FixPointNormalize(matplotlib.colors.Normalize):
    """
    Source: https://github.com/13ff6/Topography_Map_Madagascar/blob/main/Mad_Regional_Map.py
    """

    def __init__(self, vmin=None, vmax=None, sealevel=0, col_val=0.21875, clip=False):
        # sealevel is the fix point of the colormap (in data units)
        self.sealevel = sealevel
        # col_val is the color value in the range [0,1] that should represent the sealevel.
        self.col_val = col_val
        matplotlib.colors.Normalize.__init__(self, vmin, vmax, clip)

    def __call__(self, value, clip=None):
        x, y = [self.vmin, self.sealevel, self.vmax], [0, self.col_val, 1]
        return np.ma.masked_array(np.interp(value, x, y))


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

    print("Reading data...")
    upscale_factor = 1/16
    with rasterio.open('./data/ETOPO//ETOPO_2022_v1_60s_N90W180_surface.tif') as dataset:
        # resample data to target shape
        ETOPO_lowres = dataset.read(
            out_shape=(
                dataset.count,
                int(dataset.height * upscale_factor),
                int(dataset.width * upscale_factor)
            ),
        ).squeeze()

    print("Creating animation...")
    resolution = 1080

    vmax = np.max(ETOPO_lowres)
    vmin = np.min(ETOPO_lowres)

    colors_undersea = plt.cm.terrain(np.linspace(0, 0.17, 56))
    colors_land = plt.cm.terrain(np.linspace(0.25, 1, 200))

    colors = np.vstack((colors_undersea, colors_land))
    cut_terrain_map = matplotlib.colors.LinearSegmentedColormap.from_list(
        'cut_terrain', colors)
    norm = FixPointNormalize(sealevel=0, vmax=vmax, vmin=vmin)

    font = fm.FontProperties(fname='./fonts/NanumGothicCoding-Bold.ttf', size=18)

    fig, map_ax = plt.subplots()

    fig.subplots_adjust(left=0, bottom=0, right=1,
                        top=1, wspace=None, hspace=None)
    fig.set_size_inches(16/2, 9/2)
    dpi = resolution / fig.get_size_inches()[1]
    fig.set_facecolor('black')

    map_ax.axis('off')
    map_ax.set_position((0.045, 1/3-0.08, 3/4, 2/3))
    map_img = map_ax.imshow(
        ETOPO_lowres, cmap=cut_terrain_map, norm=norm, extent=(-180, 180, -90, 90))

    gradient = np.linspace(vmax, vmin, 864)
    gradient = np.vstack((gradient, gradient))
    gradient = gradient.transpose()

    color_ax = fig.add_subplot()
    color_ax.set_position((0.92, 0.2-0.08, 0.04, 0.8))
    color_ax.tick_params('y', labelcolor='white')
    color_img = color_ax.imshow(
        gradient, aspect='auto', cmap=cut_terrain_map, norm=norm, extent=(0, 1, vmin, vmax))

    text = fig.text((0.045+3/4)/2, 0.1, '', ha='center',
                    fontsize=16, color='white', fontproperties=font)

    def visualize(start, stop, step, digits=0, fps=60):
        def animate(f):
            norm = FixPointNormalize(sealevel=f, vmax=vmax, vmin=vmin)

            map_img.set_norm(norm)

            gradient = np.linspace(vmax+f, vmin+f, 864)
            gradient = np.vstack((gradient, gradient))
            gradient = gradient.transpose()
            color_img.set_norm(norm)
            color_img.set_data(gradient)
            color_img.set_extent((0, 1, vmin, vmax))

            fig.canvas.draw_idle()

            text.set_text(f'해수면 상승 높이: {f"{f:.{digits}f}m": >8}')

            return map_img, color_img, text

        anim = FuncAnimation(fig, animate, np.arange(start, stop, step))

        anim.save(f'./out/{start}_{stop}_{step}_{fps}.mp4', fps=fps, dpi=dpi)

    #visualize(0, 1000.1, 100, fps=4)
    visualize(0, 101.1, 0.1, 1, fps=30)
    visualize(0, 6021, 4, fps=30)


if __name__ == '__main__':
    main()
