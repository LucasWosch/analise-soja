import db
from services.plantio_service import PlantioService

def run():
    svc = PlantioService()

    # 2) (Opcional) Normalizar alguns registros da plantio_raw -> plantio
    #    Altere o limit para 0 para migrar tudo.
    inserted = svc.normalize_from_raw(source_table="plantio_raw", limit=0)
    print(f"[normalize_from_raw] Inseridos em plantio: {inserted}")

    # 3) Criar um registro completo (exemplo)
    novo_id = svc.create({
        "farm_id": "Fazenda A",
        "field_id": "T01",
        "date": "2025-01-15",
        "yield_kg_ha": 3450.0,
        "rain_mm": 140.0,
        "temp_mean_c": 25.1,
        "ndvi": 0.74,
        "soil_ph": 6.2,
        "soil_organic_matter": 3.1,
        "fertilizer_kg_ha": 200.0,
        "seed_rate_kg_ha": 55.0,
        "pests_incidence_pct": 8.0,
        "irrigated": 1,
        "variety": "V1",
        "crop": "soybean",
        "crop_year": 2025,
        "annual_rainfall": 140.0,
        "fertilizer": 200.0,
        "pesticide": 12.0,
        "yield_value": 3600.0
    })
    print(f"[create] novo id: {novo_id}")

    # 4) Buscar por id
    registro = svc.get(novo_id)
    print("[get]", registro)

    # 5) Atualizar
    ok = svc.update(novo_id, {"yield_kg_ha": 3550.5, "yield_value": 3650.0})
    print("[update]", ok)

    # 6) Listar com filtros e paginação
    lista = svc.list(
        filters={
            "crop": "soybean",
            "crop_year": 2025,
            "yield_min": 3000
        },
        limit=5,
        offset=0,
        order_by="yield_kg_ha",
        order_dir="DESC"
    )
    print("[list/filters] qtd:", len(lista))
    for it in lista:
        print("  ->", it)

    # 7) Agregações (GROUP BY)
    print("\n[agg] Média yield por cultura:")
    for row in svc.avg_yield_by_crop():
        print(row)

    print("\n[agg] Média yield por cultura e ano:")
    for row in svc.avg_yield_by_crop_year():
        print(row)

    print("\n[agg] Estatísticas de chuva por cultura:")
    for row in svc.rainfall_stats_by_crop():
        print(row)

    print("\n[agg] Média mensal de yield:")
    for row in svc.yield_monthly():
        print(row)

    # 8) Deletar
    deleted = svc.delete(novo_id)
    print("[delete]", deleted)

if __name__ == "__main__":
    run()