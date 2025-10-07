# plantio_repository.py
# Acesso direto ao SQLite (CRUD, filtros, paginação).

from typing import List, Optional, Tuple, Dict, Any
from models.plantio_model import Plantio
from db import get_connection

INSERT_SQL = """
INSERT INTO plantio (
  farm_id, field_id, date, yield_kg_ha, rain_mm, temp_mean_c, ndvi,
  soil_ph, soil_organic_matter, fertilizer_kg_ha, seed_rate_kg_ha,
  pests_incidence_pct, irrigated, variety, crop, crop_year,
  annual_rainfall, fertilizer, pesticide, yield
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPDATE_SQL = """
UPDATE plantio SET
  farm_id=?, field_id=?, date=?, yield_kg_ha=?, rain_mm=?, temp_mean_c=?, ndvi=?,
  soil_ph=?, soil_organic_matter=?, fertilizer_kg_ha=?, seed_rate_kg_ha=?,
  pests_incidence_pct=?, irrigated=?, variety=?, crop=?, crop_year=?,
  annual_rainfall=?, fertilizer=?, pesticide=?, yield=?
WHERE id=?
"""

SELECT_BASE = "SELECT * FROM plantio "

class PlantioRepository:

    def create(self, p: Plantio) -> int:
        with get_connection() as conn:
            cur = conn.execute(
                INSERT_SQL,
                (
                    p.farm_id, p.field_id, p.date, p.yield_kg_ha, p.rain_mm, p.temp_mean_c, p.ndvi,
                    p.soil_ph, p.soil_organic_matter, p.fertilizer_kg_ha, p.seed_rate_kg_ha,
                    p.pests_incidence_pct, p.irrigated, p.variety, p.crop, p.crop_year,
                    p.annual_rainfall, p.fertilizer, p.pesticide, p.yield_value
                )
            )
            conn.commit()
            return cur.lastrowid

    def get_by_id(self, id_: int) -> Optional[Plantio]:
        with get_connection() as conn:
            cur = conn.execute(SELECT_BASE + "WHERE id=?", (id_,))
            row = cur.fetchone()
            return Plantio.from_row(row) if row else None

    def update(self, p: Plantio) -> bool:
        if not p.id:
            raise ValueError("Plantio.id é obrigatório para update")
        with get_connection() as conn:
            cur = conn.execute(
                UPDATE_SQL,
                (
                    p.farm_id, p.field_id, p.date, p.yield_kg_ha, p.rain_mm, p.temp_mean_c, p.ndvi,
                    p.soil_ph, p.soil_organic_matter, p.fertilizer_kg_ha, p.seed_rate_kg_ha,
                    p.pests_incidence_pct, p.irrigated, p.variety, p.crop, p.crop_year,
                    p.annual_rainfall, p.fertilizer, p.pesticide, p.yield_value, p.id
                )
            )
            conn.commit()
            return cur.rowcount > 0

    def delete(self, id_: int) -> bool:
        with get_connection() as conn:
            cur = conn.execute("DELETE FROM plantio WHERE id=?", (id_,))
            conn.commit()
            return cur.rowcount > 0

    def list_all(self, limit: int = 100, offset: int = 0) -> List[Plantio]:
        with get_connection() as conn:
            cur = conn.execute(SELECT_BASE + "ORDER BY id LIMIT ? OFFSET ?", (limit, offset))
            rows = cur.fetchall()
            return [Plantio.from_row(r) for r in rows]

    def search(
        self,
        filters: Dict[str, Any],
        limit: int = 100,
        offset: int = 0,
        order_by: str = "id",
        order_dir: str = "ASC",
    ) -> List[Plantio]:
        """
        Filtros aceitos (todos opcionais): farm_id, field_id, crop, crop_year, variety,
        irrigated (0/1), date_from, date_to, yield_min, yield_max (para yield_kg_ha),
        rain_min, rain_max.
        """
        where = []
        params = []

        if "farm_id" in filters and filters["farm_id"]:
            where.append("farm_id = ?"); params.append(filters["farm_id"])
        if "field_id" in filters and filters["field_id"]:
            where.append("field_id = ?"); params.append(filters["field_id"])
        if "crop" in filters and filters["crop"]:
            where.append("crop = ?"); params.append(filters["crop"])
        if "crop_year" in filters and filters["crop_year"] is not None:
            where.append("crop_year = ?"); params.append(filters["crop_year"])
        if "variety" in filters and filters["variety"]:
            where.append("variety = ?"); params.append(filters["variety"])
        if "irrigated" in filters and filters["irrigated"] is not None:
            where.append("irrigated = ?"); params.append(filters["irrigated"])

        if "date_from" in filters and filters["date_from"]:
            where.append("date >= ?"); params.append(filters["date_from"])
        if "date_to" in filters and filters["date_to"]:
            where.append("date <= ?"); params.append(filters["date_to"])

        if "yield_min" in filters and filters["yield_min"] is not None:
            where.append("yield_kg_ha >= ?"); params.append(filters["yield_min"])
        if "yield_max" in filters and filters["yield_max"] is not None:
            where.append("yield_kg_ha <= ?"); params.append(filters["yield_max"])

        if "rain_min" in filters and filters["rain_min"] is not None:
            where.append("rain_mm >= ?"); params.append(filters["rain_min"])
        if "rain_max" in filters and filters["rain_max"] is not None:
            where.append("rain_mm <= ?"); params.append(filters["rain_max"])

        sql = SELECT_BASE
        if where:
            sql += "WHERE " + " AND ".join(where) + " "
        # proteção básica para order_by/dir
        allowed_order = {
            "id","farm_id","field_id","date","yield_kg_ha","rain_mm","temp_mean_c","ndvi","crop","crop_year"
        }
        if order_by not in allowed_order:
            order_by = "id"
        order_dir = "DESC" if str(order_dir).upper() == "DESC" else "ASC"

        sql += f"ORDER BY {order_by} {order_dir} LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with get_connection() as conn:
            cur = conn.execute(sql, tuple(params))
            rows = cur.fetchall()
            return [Plantio.from_row(r) for r in rows]

    # ===== Agregações (GROUP BY) =====

    def avg_yield_by_crop(self) -> List[dict]:
        sql = """
        SELECT crop, AVG(yield_kg_ha) AS media_yield_kg_ha, COUNT(*) AS registros
        FROM plantio
        GROUP BY crop
        ORDER BY media_yield_kg_ha DESC
        """
        with get_connection() as conn:
            cur = conn.execute(sql)
            return [dict(r) for r in cur.fetchall()]

    def avg_yield_by_crop_year(self) -> List[dict]:
        sql = """
        SELECT crop_year, crop, AVG(yield_kg_ha) AS media_yield_kg_ha, COUNT(*) AS registros
        FROM plantio
        GROUP BY crop_year, crop
        ORDER BY crop_year, media_yield_kg_ha DESC
        """
        with get_connection() as conn:
            cur = conn.execute(sql)
            return [dict(r) for r in cur.fetchall()]

    def rainfall_stats_by_crop(self) -> List[dict]:
        sql = """
        SELECT crop,
               MIN(rain_mm) AS min_rain_mm,
               MAX(rain_mm) AS max_rain_mm,
               AVG(rain_mm) AS avg_rain_mm,
               COUNT(*) AS registros
        FROM plantio
        GROUP BY crop
        ORDER BY avg_rain_mm DESC
        """
        with get_connection() as conn:
            cur = conn.execute(sql)
            return [dict(r) for r in cur.fetchall()]

    def yield_monthly(self) -> List[dict]:
        sql = """
        SELECT strftime('%Y', date) AS ano,
               strftime('%m', date) AS mes,
               AVG(yield_kg_ha) AS media_yield_kg_ha,
               COUNT(*) AS registros
        FROM plantio
        WHERE date IS NOT NULL
        GROUP BY strftime('%Y', date), strftime('%m', date)
        ORDER BY ano, mes
        """
        with get_connection() as conn:
            cur = conn.execute(sql)
            return [dict(r) for r in cur.fetchall()]
