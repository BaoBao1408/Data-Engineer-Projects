"""
src/quality/validator.py
Data quality checks for POI GeoDataFrame before loading.
Raises DataQualityError if critical checks fail.
"""
import logging
from dataclasses import dataclass, field
from typing import List
import geopandas as gpd

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    pass


@dataclass
class QualityReport:
    total: int = 0
    null_geometry: int = 0
    invalid_geometry: int = 0
    null_name_and_category: int = 0
    out_of_vietnam_bbox: int = 0
    duplicate_source_ids: int = 0
    warnings: List[str] = field(default_factory=list)
    passed: bool = True

    def summary(self) -> str:
        return (
            f"QualityReport | total={self.total} "
            f"null_geom={self.null_geometry} "
            f"invalid_geom={self.invalid_geometry} "
            f"no_name={self.null_name_and_category} "
            f"out_of_bbox={self.out_of_vietnam_bbox} "
            f"dup_ids={self.duplicate_source_ids} "
            f"passed={self.passed}"
        )


# Vietnam bounding box (approx)
VN_BBOX = (102.0, 8.0, 110.0, 24.0)  # minx, miny, maxx, maxy

# Thresholds — fail if these percentages exceeded
MAX_NULL_GEOM_PCT = 0.0       # Zero tolerance for null geometry
MAX_INVALID_GEOM_PCT = 0.05   # Max 5% invalid geometries
MAX_OUT_OF_BBOX_PCT = 0.10    # Max 10% outside Vietnam bbox
MAX_NO_NAME_PCT = 0.80        # Allow up to 80% missing names (OSM data is sparse)


class POIValidator:
    """
    Run data quality checks on a POI GeoDataFrame.
    Used both standalone and as a pipeline step before loading.
    """

    def validate(self, gdf: gpd.GeoDataFrame, raise_on_critical: bool = True) -> QualityReport:
        report = QualityReport(total=len(gdf))

        if gdf.empty:
            report.warnings.append("GeoDataFrame is empty")
            report.passed = True  # Empty is not a failure
            return report

        self._check_geometry(gdf, report)
        self._check_bbox(gdf, report)
        self._check_names(gdf, report)
        self._check_duplicate_ids(gdf, report)

        report.passed = len([w for w in report.warnings if w.startswith("CRITICAL")]) == 0

        if raise_on_critical and not report.passed:
            critical = [w for w in report.warnings if w.startswith("CRITICAL")]
            raise DataQualityError(f"Data quality checks failed: {critical}")

        logger.info(report.summary())
        return report

    def _check_geometry(self, gdf: gpd.GeoDataFrame, report: QualityReport):
        null_count = gdf.geometry.isna().sum()
        report.null_geometry = int(null_count)

        null_pct = null_count / report.total
        if null_pct > MAX_NULL_GEOM_PCT:
            report.warnings.append(
                f"CRITICAL: {null_count} records ({null_pct:.1%}) have null geometry"
            )

        invalid_count = (~gdf.geometry.is_valid).sum()
        report.invalid_geometry = int(invalid_count)
        invalid_pct = invalid_count / report.total

        if invalid_pct > MAX_INVALID_GEOM_PCT:
            report.warnings.append(
                f"CRITICAL: {invalid_count} records ({invalid_pct:.1%}) have invalid geometry"
            )
        elif invalid_count > 0:
            report.warnings.append(
                f"WARNING: {invalid_count} records have invalid geometry (will be auto-fixed)"
            )

    def _check_bbox(self, gdf: gpd.GeoDataFrame, report: QualityReport):
        minx, miny, maxx, maxy = VN_BBOX
        out_mask = (
            (gdf.geometry.x < minx) | (gdf.geometry.x > maxx) |
            (gdf.geometry.y < miny) | (gdf.geometry.y > maxy)
        )
        out_count = out_mask.sum()
        report.out_of_vietnam_bbox = int(out_count)

        out_pct = out_count / report.total
        if out_pct > MAX_OUT_OF_BBOX_PCT:
            report.warnings.append(
                f"CRITICAL: {out_count} records ({out_pct:.1%}) outside Vietnam bbox"
            )
        elif out_count > 0:
            report.warnings.append(
                f"WARNING: {out_count} records outside Vietnam bbox — will be filtered"
            )

    def _check_names(self, gdf: gpd.GeoDataFrame, report: QualityReport):
        if "name" not in gdf.columns:
            return
        if "category" not in gdf.columns:
            return

        no_name_mask = gdf["name"].isna() & (gdf["category"] == "other")
        no_name_count = no_name_mask.sum()
        report.null_name_and_category = int(no_name_count)

        pct = no_name_count / report.total
        if pct > MAX_NO_NAME_PCT:
            report.warnings.append(
                f"WARNING: {no_name_count} records ({pct:.1%}) have no name AND no category"
            )

    def _check_duplicate_ids(self, gdf: gpd.GeoDataFrame, report: QualityReport):
        if "source_id" not in gdf.columns:
            return
        dupes = gdf["source_id"].duplicated().sum()
        report.duplicate_source_ids = int(dupes)
        if dupes > 0:
            report.warnings.append(
                f"WARNING: {dupes} duplicate source_ids in batch (will be deduped)"
            )
