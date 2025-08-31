#!/usr/bin/env python3
"""
Script simple para deployment en AWS usando Python puro
"""

def main():
    print("🚀 Desplegando PyETL en AWS...")
    print("📦 Instalando dependencias AWS...")
    
    import subprocess
    import sys
    
    # Instalar dependencias si no están presentes
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "-r", "requirements-aws.txt"
        ])
    except subprocess.CalledProcessError:
        print("❌ Error instalando dependencias AWS")
        return
    
    # Ejecutar deployment
    try:
        from deploy import PyETLDeployer
        deployer = PyETLDeployer()
        if deployer.deploy():
            print("\n✅ PyETL desplegado exitosamente!")
            print("\n📝 Próximos pasos:")
            print("1. Sube un archivo .xlsx al bucket S3")
            print("2. Monitorea los logs en CloudWatch")
            print("3. Consulta los datos procesados via API")
        else:
            print("❌ Error en el deployment")
    except Exception as e:
        print(f"❌ Error ejecutando deployment: {e}")

if __name__ == "__main__":
    main()
