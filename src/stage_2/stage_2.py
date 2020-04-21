import click
import geopandas as gpd
import logging
import networkx as nx
import os
import pandas as pd
import shutil
import subprocess
import sys
import urllib.request
import uuid
import zipfile
from datetime import datetime

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
        self.stage = 2
        self.source = source.lower()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Compile database configuration variables.
        self.db_config = helpers.load_yaml(os.path.abspath("db_config.yaml"))

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def gen_dead_end(self):
        """Generates dead end junctions with NetworkX."""

        logger.info("Convert roadseg geodataframe to NetX graph.")
        graph = helpers.gdf_to_nx(self.dframes["roadseg"])

        logger.info("Create an empty graph for dead ends junctions.")
        dead_ends = nx.Graph()

        logger.info("Applying CRS EPSG:4617 to dead ends graph.")
        dead_ends.graph['crs'] = self.dframes["roadseg"].crs

        logger.info("Filter for dead end junctions.")
        dead_ends_filter = [node for node, degree in graph.degree() if degree == 1]

        logger.info("Insert filtered dead end junctions into empty graph.")
        dead_ends.add_nodes_from(dead_ends_filter)

        logger.info("Convert dead end graph to geodataframe.")
        self.dead_end_gdf = helpers.nx_to_gdf(dead_ends, nodes=True, edges=False)

        logger.info("Apply dead end junctype to junctions.")
        self.dead_end_gdf["junctype"] = "Dead End"

    def gen_intersections(self):
        """Generates intersection junction types."""

        temp_roadseg = self.dframes["roadseg"]
        temp_roadseg = temp_roadseg.drop(["uuid"], axis=1)

        logger.info("Generating junction intersections.")
        self.inter_gdf = gpd.overlay(self.dframes["roadseg"], self.dframes["roadseg"],
                                    how='intersection',
                                    keep_geom_type=False)

        self.inter_gdf["point"] = self.inter_gdf.geom_type == "Point"

        self.inter_gdf = self.inter_gdf[self.inter_gdf.point != False]

        self.inter_gdf = self.inter_gdf.drop_duplicates(subset="geometry", keep="first")

        self.inter_gdf.to_file("../../data/interim/stage_2_temp.gpkg", layer="junction", driver="GPKG")
        temp_roadseg.to_file("../../data/interim/stage_2_temp.gpkg", layer="roadseg", driver="GPKG")

        # logger.info("Calculating junction neighbours.")
        # try:
        #     subprocess.run("ogr2ogr -progress -dialect sqlite -sql 'SELECT p.*, SUM(ST_Intersects(b.geom, p.geom)) AS "
        #                    "touch FROM junction AS p, roadseg AS b GROUP BY p.fid HAVING touch <> 2' "
        #                    "../../data/interim/stage_2_temp.gpkg ../../data/interim/stage_2_temp.gpkg "
        #                    "--config OGR_SQLITE_CACHE 4096 --config OGR_SQLITE_SYNCHRONOUS OFF -nlt MULTIPOINT "
        #                    "-nln intersections -lco GEOMETRY_NAME=geom -gt 100000 -overwrite "
        #                    .format(self.source), shell=True)
        # except subprocess.CalledProcessError as e:
        #     logger.exception("Unable to calculate junction neighbours.")
        #     logger.exception("ogr2ogr error: {}".format(e))
        #     sys.exit(1)

        self.inter_gdf = gpd.read_file("../../data/interim/stage_2_temp.gpkg", layer="intersections", driver="GPKG")

        logger.info("Apply intersection junctype to junctions.")
        self.inter_gdf["junctype"] = "Intersection"
        self.inter_gdf.crs = self.dframes["roadseg"].crs
        self.inter_gdf = self.inter_gdf[["junctype", "geometry"]]

    def gen_ferry(self):
        """Generates ferry junctions with NetworkX."""

        logger.info("Convert ferryseg geodataframe to NetX graph.")
        graph = helpers.gdf_to_nx(self.dframes["ferryseg"], endpoints_only=True)

        logger.info("Convert Ferry graph to geodataframe.")
        self.ferry_gdf = helpers.nx_to_gdf(graph, nodes=True, edges=False)

        logger.info("Apply Ferry junctype to junctions.")
        self.ferry_gdf["junctype"] = "Ferry"

    def compile_target_attributes(self):
        """Compiles the target (distribution format) yaml file into a dictionary."""

        logger.info("Compiling target attribute yaml.")
        table = field = None

        # Load yaml.
        self.target_attributes = helpers.load_yaml(os.path.abspath("../distribution_format.yaml"))

        # Remove field length from dtype attribute.
        logger.info("Configuring target attributes.")
        try:

            for table in self.target_attributes:
                for field, vals in self.target_attributes[table]["fields"].items():
                    self.target_attributes[table]["fields"][field] = vals[0]

        except (AttributeError, KeyError, ValueError):
            logger.exception("Invalid schema definition for table: {}, field: {}.".format(table, field))
            sys.exit(1)

    def gen_target_junction(self):

        self.junctions = gpd.GeoDataFrame()

        self.junctions = self.junctions.assign(**{field: pd.Series(dtype=dtype) for field, dtype in
                                                  self.target_attributes["junction"]["fields"].items()})

    def combine(self):
        """Combine geodataframes."""

        logger.info("Combining ferry, dead end and intersection junctions.")
        combine = gpd.GeoDataFrame(pd.concat([self.ferry_gdf, self.dead_end_gdf, self.inter_gdf], sort=False))
        combine = combine[['junctype', 'geometry']]
        self.junctions = self.junctions.append(combine)
        self.junctions.crs = self.dframes["roadseg"].crs

    def fix_junctype(self):
        """Fix junctype of junctions outside of administrative boundaries."""

        # Download administrative boundary file.
        logger.info("Downloading administrative boundary file.")
        adm_file = helpers.load_yaml("../boundary_files.yaml")["provinces"]

        try:
            urllib.request.urlretrieve(adm_file, '../../data/raw/boundary.zip')
        except (TimeoutError, urllib.error.URLError) as e:
            logger.exception("Unable to download administrative boundary file: \"{}\".".format(adm_file))
            logger.exception(e)
            sys.exit(1)

        # Extract zipped file.
        logger.info("Extracting zipped administrative boundary file.")
        with zipfile.ZipFile("../../data/raw/boundary.zip", "r") as zip_ref:
            zip_ref.extractall("../../data/raw/boundary")

        # Transform administrative boundary file to GeoPackage layer with crs EPSG:4617.
        logger.info("Transforming administrative boundary file.")
        try:
            subprocess.run("ogr2ogr -f GPKG -sql \"SELECT * FROM lpr_000a16a_e WHERE PRUID='{}'\" "
                           "../../data/raw/boundary.gpkg ../../data/raw/boundary/lpr_000a16a_e.shp "
                           "-t_srs EPSG:4617 -nlt MULTIPOLYGON -nln {} -lco overwrite=yes "
                           .format({"ab": 48, "bc": 59, "mb": 46, "nb": 13, "nl": 10, "ns": 12, "nt": 61, "nu": 62,
                                    "on": 35, "pe": 11, "qc": 24, "sk": 47, "yt": 60}[self.source], self.source),
                           shell=True)
        except subprocess.CalledProcessError as e:
            logger.exception("Unable to transform data source to EPSG:4617.")
            logger.exception("ogr2ogr error: {}".format(e))
            sys.exit(1)

        logger.info("Remove temporary administrative boundary files and directories.")
        paths = ["../../data/raw/boundary", "../../data/raw/boundary.zip"]
        for path in paths:
            if os.path.exists(path):
                try:
                    os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)
                except OSError as e:
                    logger.warning("Unable to remove directory: \"{}\".".format(os.path.abspath(paths[0])))
                    logger.warning("OSError: {}.".format(e))
                    continue

        bound_adm = gpd.read_file("../../data/raw/boundary.gpkg", layer=self.source)
        bound_adm.crs = self.dframes["roadseg"].crs

        # Reset the junctions index.
        self.junctions = self.junctions.reset_index(drop=True)

        # Find junctions within provincial administrative boundaries.
        self.junctions = gpd.sjoin(self.junctions, bound_adm, how='left')

        # Alter junctype to NatProvTer for junctions outside provincial boundary.
        self.junctions.loc[(self.junctions.index_right.isnull()), 'junctype'] = 'NatProvTer'

        self.junctions = self.junctions.drop(["index_right"], axis=1)

        # Filter segments with an exit number.
        exitnbr_gdf = self.dframes["roadseg"][self.dframes["roadseg"]["exitnbr"] != 'None']
        exitnbr_gdf = exitnbr_gdf[["geometry", "exitnbr"]]
        self.junctions = gpd.sjoin(self.junctions, exitnbr_gdf, how='left', op='intersects')
        self.junctions = self.junctions.rename(columns={"exitnbr_right": "exitnbr"})

        self.junctions = self.junctions.drop(["exitnbr_left",
                                              "PRUID",
                                              "PRNAME",
                                              "PRENAME",
                                              "PRFNAME",
                                              "PREABBR",
                                              "PRFABBR",
                                              "index_right"], axis=1)

        self.junctions = self.junctions.drop_duplicates(subset="geometry", keep="first")

    def gen_junctions(self):
        """Generate final dataset."""

        # Set standard field values.
        self.junctions["uuid"] = [uuid.uuid4().hex for _ in range(len(self.junctions))]
        self.junctions["credate"] = datetime.today().strftime("%Y%m%d")
        self.junctions["datasetnam"] = self.dframes["roadseg"]["datasetnam"][0]
        self.junctions["credate"] = datetime.today().strftime("%Y%m%d")
        self.junctions["metacover"] = "Complete"
        self.junctions["acqtech"] = "Computed"
        self.junctions["provider"] = "Federal"

        self.dframes["junction"] = self.junctions
        self.dframes["junction"] = self.dframes["junction"].drop_duplicates(subset="geometry", keep="first")

        # Apply field domains.
        self.apply_domains()

        # Convert geometry from multipoint to point.
        if self.dframes["junction"].geom_type[0] == "MultiPoint":
            self.multipoint_to_point()

    def apply_domains(self):
        """Applies the field domains to each column in the target dataframes."""

        logging.info("Applying field domains to junction.")
        defaults = helpers.compile_default_values()
        dtypes = helpers.compile_dtypes()
        field = None

        try:

            for field, domains in defaults["junction"].items():

                logger.info("Target field \"{}\": Applying domain.".format(field))

                # Apply domains to dataframe.
                default = defaults["junction"][field]
                self.dframes["junction"][field] = self.dframes["junction"][field].map(
                    lambda val: default if val == "" or pd.isna(val) else val)

                # Force adjust data type.
                self.dframes["junction"][field] = self.dframes["junction"][field].astype(dtypes["junction"][field])

        except (AttributeError, KeyError, ValueError):
            logger.exception("Invalid schema definition for table: junction, field: {}.".format(field))
            sys.exit(1)

    def multipoint_to_point(self):
        """Converts junction geometry from multipoint to point."""

        self.dframes["junction"]["geometry"] = self.dframes["junction"]["geometry"].map(lambda geom: geom[0])

    def export_gpkg(self):
        """Exports the junctions dataframe as a GeoPackage layer."""

        logger.info("Exporting junctions dataframe to GeoPackage layer.")

        # Export junctions dataframe to GeoPackage layer.
        # helpers.export_gpkg({"junction": self.dframes["junction"]}, self.data_path)
        self.dframes["junction"].to_file("../../data/interim/nb.gpkg", driver="GPKG", layer="junction")

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.gen_dead_end()
        self.gen_intersections()
        self.gen_ferry()
        self.compile_target_attributes()
        self.gen_target_junction()
        self.combine()
        self.fix_junctype()
        self.gen_junctions()
        self.export_gpkg()


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
