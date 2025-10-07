from dataclasses import dataclass
from typing import Optional

@dataclass
class Plantio:
    id: Optional[int] = None
    farm_id: Optional[str] = None
    field_id: Optional[str] = None
    date: Optional[str] = None  # ISO string "YYYY-MM-DD" (opcional)
    yield_kg_ha: Optional[float] = None
    rain_mm: Optional[float] = None
    temp_mean_c: Optional[float] = None
    ndvi: Optional[float] = None
    soil_ph: Optional[float] = None
    soil_organic_matter: Optional[float] = None
    fertilizer_kg_ha: Optional[float] = None
    seed_rate_kg_ha: Optional[float] = None
    pests_incidence_pct: Optional[float] = None
    irrigated: Optional[int] = None  # 0/1
    variety: Optional[str] = None
    # Campos extras (compatibilidade com bases alternativas)
    crop: Optional[str] = None
    crop_year: Optional[int] = None
    annual_rainfall: Optional[float] = None
    fertilizer: Optional[float] = None
    pesticide: Optional[float] = None
    yield_value: Optional[float] = None  # 'yield' é palavra reservada no Python, usamos yield_value

    @staticmethod
    def from_row(row) -> "Plantio":
        # 'yield' no banco vira yield_value no objeto
        return Plantio(
            id=row["id"] if "id" in row.keys() else None,
            farm_id=row["farm_id"] if "farm_id" in row.keys() else None,
            field_id=row["field_id"] if "field_id" in row.keys() else None,
            date=row["date"] if "date" in row.keys() else None,
            yield_kg_ha=row["yield_kg_ha"] if "yield_kg_ha" in row.keys() else None,
            rain_mm=row["rain_mm"] if "rain_mm" in row.keys() else None,
            temp_mean_c=row["temp_mean_c"] if "temp_mean_c" in row.keys() else None,
            ndvi=row["ndvi"] if "ndvi" in row.keys() else None,
            soil_ph=row["soil_ph"] if "soil_ph" in row.keys() else None,
            soil_organic_matter=row["soil_organic_matter"] if "soil_organic_matter" in row.keys() else None,
            fertilizer_kg_ha=row["fertilizer_kg_ha"] if "fertilizer_kg_ha" in row.keys() else None,
            seed_rate_kg_ha=row["seed_rate_kg_ha"] if "seed_rate_kg_ha" in row.keys() else None,
            pests_incidence_pct=row["pests_incidence_pct"] if "pests_incidence_pct" in row.keys() else None,
            irrigated=row["irrigated"] if "irrigated" in row.keys() else None,
            variety=row["variety"] if "variety" in row.keys() else None,
            crop=row["crop"] if "crop" in row.keys() else None,
            crop_year=row["crop_year"] if "crop_year" in row.keys() else None,
            annual_rainfall=row["annual_rainfall"] if "annual_rainfall" in row.keys() else None,
            fertilizer=row["fertilizer"] if "fertilizer" in row.keys() else None,
            pesticide=row["pesticide"] if "pesticide" in row.keys() else None,
            yield_value=row["yield"] if "yield" in row.keys() else None,
        )
