import ray
import glob
from limpieza import limpiar_datos
import analytics

ray.init(address="auto")

def main():
    # Obtener años disponibles
    rutas = sorted(glob.glob("data/parquet/atus_anual_*.parquet"))
    
    for ruta in rutas:
        ano = ruta.split("_")[-1].split(".")[0]
        print(f"\n{'='*60}")
        print(f"📊 AÑO {ano}")
        print(f"{'='*60}")
        
        df = limpiar_datos(ruta)
        
        print(f"\n✅ Accidentes: {len(df):,}")
        print(f"✅ Muertos: {int(df['TOTAL_MUERTOS'].sum()):,}")
        print(f"✅ Heridos: {int(df['TOTAL_HERIDOS'].sum()):,}")
        
        print(f"\n🏆 TOP ESTADOS:")
        for entidad, count in analytics.estados_mas_accidentes(df)[:5]:
            print(f"   E{entidad}: {count:,}")


if __name__ == "__main__":
    main()