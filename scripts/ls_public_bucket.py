import logging
import re
import uuid
from multiprocessing import Manager, Process, cpu_count, current_process
from queue import Empty
import os

import boto3
import click
import datacube
from botocore import UNSIGNED
from botocore.config import Config
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes
from osgeo import osr

from copy import deepcopy

from collections import Counter 

import json


from odc.index import eo3_grid_spatial, odc_uuid

# Need to check if we're on new gdal for coordinate order
import osgeo.gdal
from packaging import version

from ruamel.yaml import YAML

GUARDIAN = "GUARDIAN_QUEUE_EMPTY"
AWS_PDS_TXT_SUFFIX = "MTL.txt"

LON_LAT_ORDER = version.parse(osgeo.gdal.__version__) < version.parse("3.0.0")


MTL_PAIRS_RE = re.compile(r'(\w+)\s=\s(.*)')

bands_ls8 = [('1', 'coastal_aerosol'),
             ('2', 'blue'),
             ('3', 'green'),
             ('4', 'red'),
             ('5', 'nir'),
             ('6', 'swir1'),
             ('7', 'swir2'),
             ('8', 'panchromatic'),
             ('9', 'cirrus'),
             ('10', 'lwir1'),
             ('11', 'lwir2'),
             ('QUALITY', 'quality')]

bands_ls7 = [('1', 'blue'),
             ('2', 'green'),
             ('3', 'red'),
             ('4', 'nir'),
             ('5', 'swir1'),
             ('7', 'swir2'),
             ('QUALITY', 'quality')]

bands_s2 = {
    "B01": 'coastal_aerosol',
    "B02": 'blue',
    "B03": 'green',
    "B04": 'red',
    "B05": 'red_edge_1',
    "B06": 'red_edge_2',
    "B07": 'red_edge_3',
    "B08": 'nir_1',
    "B8A": 'nir_2',
    "B09": 'water_vapour',
    "B11": 'swir_1',
    "B12": 'swir_2',
    "SCL": 'scl'
}


# STACproduct lookup
def _stac_lookup(item):
    # "sentinel:product_id": "S2A_MSIL2A_20191203T102401_N0213_R065_T30NYN_20191203T121856"
    product_id = "unknown"
    product_type = "unknown"
    region_code = "nnTTT"

    if "sentinel:product_id" in item.properties:
        product_id = item.properties["sentinel:product_id"]
        product_split = product_id.split("_")
        if product_split[0] in ["S2A", "S2B"]:
            # product = "Sentinel-2"
            product_type = "{}_{}".format(product_split[0], product_split[1])
            region_code = "{}{}{}".format(
                str(item.properties["proj:epsg"])[-2:],
                item.properties["sentinel:latitude_band"],
                item.properties["sentinel:grid_square"]
            )
    else:
        logging.error("Failed to recognise product.")

    return product_id, product_type, region_code


def _parse_value(s):
    s = s.strip('"')
    for parser in [int, float]:
        try:
            return parser(s)
        except ValueError:
            pass
    return s


def _parse_group(lines):
    tree = {}
    for line in lines:
        match = MTL_PAIRS_RE.findall(line)
        if match:
            key, value = match[0]
            if key == 'GROUP':
                tree[value] = _parse_group(lines)
            elif key == 'END_GROUP':
                break
            else:
                tree[key] = _parse_value(value)
    return tree


def get_geo_ref_points(info):
    return {
        'ul': {'x': info['CORNER_UL_PROJECTION_X_PRODUCT'], 'y': info['CORNER_UL_PROJECTION_Y_PRODUCT']},
        'ur': {'x': info['CORNER_UR_PROJECTION_X_PRODUCT'], 'y': info['CORNER_UR_PROJECTION_Y_PRODUCT']},
        'll': {'x': info['CORNER_LL_PROJECTION_X_PRODUCT'], 'y': info['CORNER_LL_PROJECTION_Y_PRODUCT']},
        'lr': {'x': info['CORNER_LR_PROJECTION_X_PRODUCT'], 'y': info['CORNER_LR_PROJECTION_Y_PRODUCT']},
    }


def get_stac_geo_ref_points(bounds):
    return {
        'ul': {'x': bounds.left, 'y': bounds.top},
        'ur': {'x': bounds.right, 'y': bounds.top},
        'll': {'x': bounds.left, 'y': bounds.bottom},
        'lr': {'x': bounds.right, 'y': bounds.bottom},
    }


def get_coords(geo_ref_points, spatial_ref):
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        # GDAL 3 reverses coordinate order, because... standards
        if LON_LAT_ORDER:
            # GDAL 2.0 order
            lon, lat, z = t.TransformPoint(p['x'], p['y'])
        else:
            # GDAL 3.0 order
            lat, lon, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}

def geographic_to_projected(geometry, target_srs):
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(4326)

    target_ref = osr.SpatialReference()
    target_ref.ImportFromEPSG(target_srs)

    t = osr.CoordinateTransformation(spatial_ref, target_ref)

    def transform(p):
        # GDAL 3 reverses coordinate order, because... standards
        if LON_LAT_ORDER:
            # GDAL 2.0 order
            x, y, z = t.TransformPoint(p[0], p[1])
        else:
            # GDAL 3.0 order
            y, x, z = t.TransformPoint(p[1], p[0])
        return [x, y]

    new_geometry = deepcopy(geometry)
    new_geometry['coordinates'][0] = [transform(p) for p in new_geometry['coordinates'][0]]

    return new_geometry


def satellite_ref(sat):
    """
    To load the band_names for referencing either LANDSAT8 or LANDSAT7 bands
    """
    if sat == 'LANDSAT_8':
        sat_img = bands_ls8
    elif sat == 'LANDSAT_7' or sat == 'LANDSAT_5':
        sat_img = bands_ls7
    else:
        raise ValueError('Satellite data Not Supported')
    return sat_img


def absolutify_paths(doc, bucket_name, obj_key):
    objt_key = format_obj_key(obj_key)
    for band in doc['image']['bands'].values():
        band['path'] = get_s3_url(bucket_name, objt_key + '/' + band['path'])
    return doc


def relativise_path(href):
    return os.path.split(href)[1]


def get_stac_bands(item, default_grid='g10m'):
    bands = {}

    grids = {}

    assets = item.assets

    for band in bands_s2:
        asset = assets[band]
        transform = asset['proj:transform']
        grid = "g{}m".format(transform[0])

        if grid not in grids:
            grids[grid] = {
                'shape': asset['proj:shape'],
                'transform': asset['proj:transform']
            }

        band_info = {
            'path': relativise_path(asset['href']),
        }

        if grid != default_grid:
           band_info['grid'] = grid

        bands[bands_s2[band]] = band_info

    grids['default'] = grids[default_grid]
    del grids[default_grid]

    return bands, grids


def make_stac_metadata_doc(item):

    # Dodgy lookup
    product_id, product_type, region_code = _stac_lookup(item)

    # Make a proper deterministic UUID
    deterministic_uuid = str(odc_uuid("sentinel2_stac_process", "1.0.0", [product_id]))

    # Get grids and bands
    bands, grids = get_stac_bands(item)

    doc = {
        '$schema': 'https://schemas.opendatacube.org/dataset',
        'id': deterministic_uuid,
        'crs': "epsg:{}".format(item.properties['proj:epsg']),
        'geometry': geographic_to_projected(item.geometry, item.properties['proj:epsg']),
        'grids': grids,
        'product': {
            'name': product_type.lower()  # This is not right
        },
        'label': product_id,
        'properties': {
            'datetime': item.properties['datetime'].replace("000+00:00", "Z"),
            'odc:processing_datetime': item.properties['datetime'].replace("000+00:00", "Z"),
            'eo:cloud_cover': item.properties['eo:cloud_cover'],
            'eo:gsd': item.properties['gsd'],
            'eo:instrument': item.properties['instruments'][0],
            'eo:platform': item.properties['platform'],
            'odc:file_format': 'GeoTIFF',
            'odc:region_code': region_code
        },
        'measurements': bands,
        'lineage': {}
    }

    # with open(f'/opt/odc/data/{item}.json', 'w') as outfile:
    #     json.dump(doc, outfile, indent=4)

    return dict(**doc,
                **eo3_grid_spatial(doc))


def make_metadata_doc(mtl_data, bucket_name, object_key):
    mtl_product_info = mtl_data['PRODUCT_METADATA']
    mtl_metadata_info = mtl_data['METADATA_FILE_INFO']
    satellite = mtl_product_info['SPACECRAFT_ID']
    instrument = mtl_product_info['SENSOR_ID']
    acquisition_date = mtl_product_info['DATE_ACQUIRED']
    scene_center_time = mtl_product_info['SCENE_CENTER_TIME']
    level = mtl_product_info['DATA_TYPE']
    product_type = 'L1TP'
    sensing_time = acquisition_date + ' ' + scene_center_time
    cs_code = 32600 + mtl_data['PROJECTION_PARAMETERS']['UTM_ZONE']
    label = mtl_metadata_info['LANDSAT_SCENE_ID']
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(cs_code)
    geo_ref_points = get_geo_ref_points(mtl_product_info)
    coordinates = get_coords(geo_ref_points, spatial_ref)
    bands = satellite_ref(satellite)
    doc = {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, get_s3_url(bucket_name, object_key))),
        'processing_level': level,
        'product_type': product_type,
        'creation_dt': str(acquisition_date),
        'label': label,
        'platform': {'code': satellite},
        'instrument': {'name': instrument},
        'extent': {
            'from_dt': sensing_time,
            'to_dt': sensing_time,
            'center_dt': sensing_time,
            'coord': coordinates,
        },
        'format': {'name': 'GeoTiff'},
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': 'EPSG:%s' % cs_code,
            }
        },
        'image': {
            'bands': {
                band[1]: {
                    'path': mtl_product_info['FILE_NAME_BAND_' + band[0]],
                    'layer': 1,
                } for band in bands
            }
        },
        'lineage': {'source_datasets': {}},
    }
    doc = absolutify_paths(doc, bucket_name, object_key)
    return doc


def format_obj_key(obj_key):
    obj_key = '/'.join(obj_key.split("/")[:-1])
    return obj_key


def get_s3_url(bucket_name, obj_key):
    return 's3://{bucket_name}/{obj_key}'.format(
        bucket_name=bucket_name, obj_key=obj_key)


def archive_document(doc, uri, index, sources_policy):
    def get_ids(dataset):
        ds = index.datasets.get(dataset.id, include_sources=True)
        for source in ds.sources.values():
            yield source.id
        yield dataset.id

    resolver = Doc2Dataset(index)
    dataset, _ = resolver(doc, uri)
    index.datasets.archive(get_ids(dataset))
    logging.info("Archiving %s and all sources of %s", dataset.id, dataset.id)


def add_dataset(doc, uri, index, sources_policy):
    logging.info("Indexing %s", uri)
    resolver = Doc2Dataset(index)
    dataset, err = resolver(doc, uri)

    existing_dataset = index.datasets.get(doc['id'])

    if not existing_dataset:
        logging.info("Indexing dataset...")
        if err is not None:
            logging.error("%s", err)
        else:
            try:
                index.datasets.add(dataset, with_lineage=False)
            except Exception as e:
                logging.error("Unhandled exception %s", e)
    else:
        logging.info("Updating dataset...")
        try:
            index.datasets.update(dataset, {tuple(): changes.allow_any})
        except Exception as e:
            logging.error("Unhandled exception %s", e)
    logging.info(f"Dataset {doc['id']} indexed.")
    return dataset, err


def worker(config, bucket_name, prefix, suffix, start_date, end_date, func, unsafe, sources_policy, queue):
    dc = datacube.Datacube(config=config)
    index = dc.index
    s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
    safety = 'safe' if not unsafe else 'unsafe'

    while True:
        try:
            key = queue.get(timeout=60)
            if key == GUARDIAN:
                break
            logging.info("Processing %s %s", key, current_process())
            obj = s3.Object(bucket_name, key).get()
            raw = obj['Body'].read()
            if suffix == AWS_PDS_TXT_SUFFIX:
                # Attempt to process text document
                raw_string = raw.decode('utf8')
                txt_doc = _parse_group(iter(raw_string.split("\n")))['L1_METADATA_FILE']
                data = make_metadata_doc(txt_doc, bucket_name, key)
            else:
                yaml = YAML(typ=safety, pure=False)
                yaml.default_flow_style = False
                data = yaml.load(raw)
            uri = get_s3_url(bucket_name, key)
            cdt = data['creation_dt']
            # Use the fact lexicographical ordering matches the chronological ordering
            if cdt >= start_date and cdt < end_date:
                logging.info("calling %s", func)
                func(data, uri, index, sources_policy)
            queue.task_done()
        except Empty:
            break
        except EOFError:
            break


def iterate_datasets(bucket_name, config, prefix, suffix, start_date, end_date, func, unsafe, sources_policy):
    manager = Manager()
    queue = manager.Queue()

    s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
    bucket = s3.Bucket(bucket_name)
    logging.info("Bucket : %s prefix: %s ", bucket_name, str(prefix))
    # safety = 'safe' if not unsafe else 'unsafe'
    worker_count = cpu_count() * 2

    processess = []
    for i in range(worker_count):
        proc = Process(target=worker, args=(config, bucket_name, prefix, suffix, start_date, end_date, func, unsafe, sources_policy, queue,))
        processess.append(proc)
        proc.start()

    for obj in bucket.objects.filter(Prefix=str(prefix)):
        if (obj.key.endswith(suffix)):
            queue.put(obj.key)

    for i in range(worker_count):
        queue.put(GUARDIAN)

    for proc in processess:
        proc.join()


@click.command(help="Enter Bucket name. Optional to enter configuration file to access a different database")
@click.argument('bucket_name')
@click.option(
    '--config',
    '-c',
    help="Pass the configuration file to access the database",
    type=click.Path(exists=True)
)
@click.option('--prefix', '-p', help="Pass the prefix of the object to the bucket")
@click.option('--suffix', '-s', default=".yaml", help="Defines the suffix of the metadata_docs that will be used to load datasets. For AWS PDS bucket use MTL.txt")
@click.option('--start_date', help="Pass the start acquisition date, in YYYY-MM-DD format")
@click.option('--end_date', help="Pass the end acquisition date, in YYYY-MM-DD format")
@click.option('--archive', is_flag=True, help="If true, datasets found in the specified bucket and prefix will be archived")
@click.option('--unsafe', is_flag=True, help="If true, YAML will be parsed unsafely. Only use on trusted datasets. Only valid if suffix is yaml")
@click.option('--sources_policy', default="verify", help="verify, ensure, skip")
def main(bucket_name, config, prefix, suffix, start_date, end_date, archive, unsafe, sources_policy):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    action = archive_document if archive else add_dataset
    iterate_datasets(bucket_name, config, prefix, suffix, start_date, end_date, action, unsafe, sources_policy)


if __name__ == "__main__":
    main()
