# plantio_service.py
# Camada de serviço: orquestra repositório, faz ETL simples e validações.

from typing import List, Dict, Any, Optional
from models.plantio_model import Plantio
from repositories.plantio_repository import PlantioRepository
from db import get_connection

class PlantioService:
    def __init__(self):
        self.repo = PlantioRepository()

    # ===== CRUD =====

    def create(self, data: Dict[str, Any]) -> int:
        p = self._from_dict(data)
        return self.repo.create(p)

    def get(self, id_: int) -> Optional[Plantio]:
        return self.repo.get_by_id(id_)

    def update(self, id_: int, data: Dict[str, Any]) -> bool:
        existing = self.repo.get_by_id(id_)
        if not existing:
            return False
        # mescla campos
        merged = self._merge(existing, data)
        return self.repo.update(merged)

    def delete(self, id_: int) -> bool:
        return self.repo.delete(id_)

    def list(self, filters: Dict[str, Any], limit: int = 100, offset: int = 0, order_by: str = "id", order_dir: str = "ASC") -> List[Plantio]:
        return self.repo.search(filters, limit=limit, offset=offset, order_by=order_by, order_dir=order_dir)

    # ===== Agregações =====

    def avg_yield_by_crop(self) -> List[Dict[str, Any]]:
        return self.repo.avg_yield_by_crop()

    def avg_yield_by_crop_year(self) -> List[Dict[str, Any]]:
        return self.repo.avg_yield_by_crop_year()

    def rainfall_stats_by_crop(self) -> List[Dict[str, Any]]:
        return self.repo.rainfall_stats_by_crop()

    def yield_monthly(self) -> List[Dict[str, Any]]:
        return self.repo.yield_monthly()

    # ===== ETL simples: migrar da plantio_raw (se existir) para a plantio normalizada =====
    # Mapeia colunas comuns: crop/crop_year/annual_rainfall/fertilizer/pesticide/yield -> campos normalizados.

    def normalize_from_raw(self, source_table: str = "plantio_raw", limit: int = 0) -> int:
        """
        Lê registros de 'plantio_raw' e insere em 'plantio' com mapeamento para campos normalizados.
        - Se limit > 0, processa apenas esse número de linhas (útil para testes).
        Retorna quantidade inserida.
        """
        with get_connection() as conn:
            # verifica se a tabela existe
            r = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (source_table,)).fetchone()
            if not r:
                return 0

            base_sql = f"SELECT * FROM {source_table}"
            if limit > 0:
                base_sql += f" LIMIT {limit}"
            rows = conn.execute(base_sql).fetchall()

        inserted = 0
        for row in rows:
            d = dict(row)
            # mapeia com segurança (se não houver a chave, fica None)
            data = {
                "farm_id": d.get("farm_id"),
                "field_id": d.get("field_id"),
                "date": d.get("date"),  # se existir
                "yield_kg_ha": d.get("yield_kg_ha"),
                "rain_mm": d.get("rain_mm") or d.get("annual_rainfall"),
                "temp_mean_c": d.get("temp_mean_c"),
                "ndvi": d.get("ndvi"),
                "soil_ph": d.get("soil_ph"),
                "soil_organic_matter": d.get("soil_organic_matter"),
                "fertilizer_kg_ha": d.get("fertilizer_kg_ha") or d.get("fertilizer"),
                "seed_rate_kg_ha": d.get("seed_rate_kg_ha"),
                "pests_incidence_pct": d.get("pests_incidence_pct"),
                "irrigated": d.get("irrigated"),
                "variety": d.get("variety"),
                "crop": d.get("crop"),
                "crop_year": d.get("crop_year"),
                "annual_rainfall": d.get("annual_rainfall"),
                "fertilizer": d.get("fertilizer"),
                "pesticide": d.get("pesticide"),
                "yield_value": d.get("yield")  # cuidado: 'yield' é coluna comum em alguns CSVs
            }
            self.repo.create(self._from_dict(data))
            inserted += 1

        return inserted

    # ===== helpers internos =====

    def _from_dict(self, data: Dict[str, Any]) -> Plantio:
        # aceita tanto 'yield' quanto 'yield_value' no dict de entrada
        yv = data.get("yield_value", data.get("yield"))
        return Plantio(
            id=data.get("id"),
            farm_id=data.get("farm_id"),
            field_id=data.get("field_id"),
            date=data.get("date"),
            yield_kg_ha=data.get("yield_kg_ha"),
            rain_mm=data.get("rain_mm"),
            temp_mean_c=data.get("temp_mean_c"),
            ndvi=data.get("ndvi"),
            soil_ph=data.get("soil_ph"),
            soil_organic_matter=data.get("soil_organic_matter"),
            fertilizer_kg_ha=data.get("fertilizer_kg_ha"),
            seed_rate_kg_ha=data.get("seed_rate_kg_ha"),
            pests_incidence_pct=data.get("pests_incidence_pct"),
            irrigated=data.get("irrigated"),
            variety=data.get("variety"),
            crop=data.get("crop"),
            crop_year=data.get("crop_year"),
            annual_rainfall=data.get("annual_rainfall"),
            fertilizer=data.get("fertilizer"),
            pesticide=data.get("pesticide"),
            yield_value=yv
        )

    def _merge(self, entity: Plantio, data: Dict[str, Any]) -> Plantio:
        # sobrescreve apenas os campos presentes em data
        for field in entity.__dataclass_fields__.keys():
            if field in data:
                setattr(entity, field, data[field])
        # se passar 'yield' crua, converte para yield_value
        if "yield" in data and "yield_value" not in data:
            entity.yield_value = data["yield"]
        return entity
