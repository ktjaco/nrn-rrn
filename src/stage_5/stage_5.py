import click
import geopandas as gpd
import logging
import os
import urllib.request
import uuid
import subprocess
import sys
import zipfile
from sqlalchemy import *
from shapely.ops import split, snap, linemerge, unary_union

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers

# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class Stage:
    """Defines an NRN stage."""

    def __init__(self, source):
        self.stage = 5
        self.source = source.lower()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

    def load_gpkg(self):
        """Loads input GeoPackage layers into data frames."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def dl_latest_vintage(self):
        """Downloads the latest provincial NRN data set from Open Maps/FGP."""

        # Download latest vintage data set.
        logger.info("Downloading latest provincial dataset.")
        vintage = "https://geoprod.statcan.gc.ca/nrn_rrn/nb/NRN_RRN_NB_9_0_SHP.zip"
        urllib.request.urlretrieve(vintage, '../../data/raw/vintage.zip')
        with zipfile.ZipFile("../../data/raw/vintage.zip", "r") as zip_ref:
            zip_ref.extractall("../../data/raw/vintage")

        # Transform latest vintage into crs EPSG:4617.
        logger.info("Transforming latest provincial dataset.")
        try:
            subprocess.run('ogr2ogr -f "ESRI Shapefile" -t_srs EPSG:4617 '
                           '../../data/raw/vintage/NRN_RRN_NB_9_0_SHP/NRN_NB_9_0_SHP_en/4617 '
                           '../../data/raw/vintage/NRN_RRN_NB_9_0_SHP/NRN_NB_9_0_SHP_en')
        except subprocess.CalledProcessError as e:
            logger.exception("Unable to transform data source to EPSG:4617.")
            logger.exception("ogr2ogr error: {}".format(e))
            sys.exit(1)

        logger.info("Reading latest provincial dataset.")
        self.vintage_roadseg = gpd.read_file("../../data/raw/vintage/NRN_RRN_NB_9_0_SHP/NRN_NB_9_0_SHP_en/4617/NRN_NB_9_0_ROADSEG.shp")
        self.vintage_ferryseg = gpd.read_file("../../data/raw/vintage/NRN_RRN_NB_9_0_SHP/NRN_NB_9_0_SHP_en/4617/NRN_NB_9_0_FERRYSEG.shp")
        self.vintage_junction = gpd.read_file("../../data/raw/vintage/NRN_RRN_NB_9_0_SHP/NRN_NB_9_0_SHP_en/4617/NRN_NB_9_0_JUNCTION.shp")

    def split_line_by_nearest_points(self, gdf_line, gdf_points, tolerance):
        """
        Split the union of lines with the union of points resulting
        Parameters
        ----------
        gdf_line : geoDataFrame
            geodataframe with multiple rows of connecting line segments
        gdf_points : geoDataFrame
            geodataframe with multiple rows of single points

        Returns
        -------
        gdf_segments : geoDataFrame
            geodataframe of segments
        """

        gdf_line["diss"] = "1"
        line = gdf_line.dissolve(by="diss")

        # union all geometries
        line = gdf_line.geometry.unary_union
        line = linemerge(line)
        coords = gdf_points.geometry.unary_union

        # snap and split coords on line
        # returns GeometryCollection
        split_line = split(line, snap(coords, line, tolerance))

        # transform Geometry Collection to GeoDataFrame
        segments = [feature for feature in split_line]

        gdf_segments = gpd.GeoDataFrame(
            list(range(len(segments))), geometry=segments)
        gdf_segments.columns = ['index', 'geometry']

        return gdf_segments

    def roadseg_equality(self):
        """Checks if roadseg features have equal geometry."""

        # self.nb_old = gpd.read_file("../../data/interim/nb_test.gpkg", layer="old")
        # self.nb_new = gpd.read_file("../../data/interim/nb_test.gpkg", layer="new")

        logger.info("Checking for road segment geometry equality.")
        # Returns True or False to a new column if geometry is equal.
        self.dframes["roadseg"]["equals"] = self.dframes["roadseg"].geom_equals(self.vintage_roadseg)
        # self.nb_new["equals"] = self.nb_new.geom_equals(self.nb_old)

        logger.info("Logging geometry equality.")
        for index, row in self.dframes["roadseg"].iterrows():

            # Logs uuid of equal geometry and applies the nid from vintage to newest data.
            if row['equals'] == 1:
                logger.warning("Equal roadseg geometry detected for uuid: {}".format(index))

                # Apply NID from latest vintage to newest data.
                self.dframes["roadseg"]["nid"] = self.vintage_roadseg["nid"]
                # self.dframes["roadseg"]["nid"] = "Equal"

            else:

                # Logs uuid of equal geometry.
                if row["equals"] == 0:
                    logger.warning("Unequal roadseg geometry detected for uuid: {}".format(index))

                    self.dframes["roadseg"]["nid"] = ""

                    # This is temporary. The same NID will have to be applied to segments between junctions.
                    # self.dframes["roadseg"]["nid"] = [uuid.uuid4().hex for _ in range(len(self.dframes["roadseg"]))]

        # self.nb_new.to_file("../../data/interim/nb_test.gpkg", layer="test", driver="GPKG")

        split_lines = self.split_line_by_nearest_points(self.dframes["roadseg"], self.dframes["junction"], tolerance=0.5)

        logger.info("Writing test road segment GPKG.")
        helpers.export_gpkg({"roadseg_equal": self.dframes["roadseg"]}, self.data_path)
        helpers.export_gpkg({"split_lines": split_lines}, self.data_path)

        sys.exit(1)

    def ferryseg_equality(self):
        """Checks if ferryseg features have equal geometry."""

        logger.info("Checking for ferry segment geometry equality.")
        # Returns True or False to a new column if geometry is equal.
        self.dframes["ferryseg"]["equals"] = self.dframes["ferryseg"].geom_equals(self.vintage_ferryseg)

        logger.info("Logging geometry equality.")
        for index, row in self.dframes["ferryseg"].iterrows():

            if row['equals'] == 1:
                logger.warning("Equal ferryseg geometry detected for uuid: {}".format(index))

            else:
                logger.warning("Unequal ferryseg geometry detected for uuid: {}".format(index))

        logger.info("Writing test ferry segment GPKG.")
        helpers.export_gpkg({"ferryseg_equal": self.dframes["ferryseg"]}, self.data_path)

    def junction_equality(self):
        """Checks if junction features have equal geometry."""

        logger.info("Checking for junction geometry equality.")
        # Returns True or False to a new column if geometry is equal.
        self.dframes["junction"]["equals"] = self.dframes["junction"].geom_equals(self.vintage_junction)

        logger.info("Logging geometry equality.")
        for index, row in self.dframes["junction"].iterrows():

            if row['equals'] == 1:
                logger.warning("Equal junction geometry detected for uuid: {}".format(index))

            else:
                logger.warning("Unequal junction geometry detected for uuid: {}".format(index))

        logger.info("Writing test junction GPKG.")
        helpers.export_gpkg({"junction_equal": self.dframes["junction"]}, self.data_path)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.dl_latest_vintage()
        self.roadseg_equality()
        self.ferryseg_equality()
        self.junction_equality()

@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt parks_canada".split(), False))
def main(source):
    """Executes an NRN stage."""

    try:

        with helpers.Timer():
            stage = Stage(source)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
