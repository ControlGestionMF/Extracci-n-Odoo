import xmlrpc.client
import pandas as pd

# Parámetros de conexión a Odoo
url = 'https://movingfood.konos.cl'
db = 'movingfood-mfood-erp-main-7481157'
username = 'logistica@movingfood.cl'
api_key = '7a1e4e24b1f34abbe7c6fd93fd5fd75dccda90a6'

# Establecer conexión XML-RPC con Odoo
def conectar_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, api_key, {})
    if not uid:
        raise Exception('Error de autenticación en Odoo')
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

# Función para extraer productos (modelo product.product)
def extraer_productos(uid, models):
    fields = [
        'id', 'default_code', 'name', 'uom_id', 
        'list_price', 'standard_price', 'sale_ok', 'create_date'
    ]
    
    productos = models.execute_kw(
        db, uid, api_key,
        'product.product', 'search_read',
        [[],], {'fields': fields}
    )
    df = pd.DataFrame(productos)
    
    # Procesar campos Many2one
    df['uom_id_id'] = df['uom_id'].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
    df['uom_id_name'] = df['uom_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    
    # Mapear columnas al esquema DataWarehouse
    df_final = df.rename(columns={
        'id': 'id_producto',
        'default_code': 'referencia_interna',
        'name': 'nombre_producto',
        'uom_id_name': 'unidad_medida',
        'list_price': 'precio_unitario',
        'standard_price': 'coste_unitario',
        'sale_ok': 'puede_ser_vendido',
        'create_date': 'fecha_creacion'
    })
    
    # Seleccionar y reordenar columnas
    df_final = df_final[[ 
        'id_producto', 'referencia_interna', 'nombre_producto', 
        'unidad_medida', 'precio_unitario', 'coste_unitario',
        'puede_ser_vendido', 'fecha_creacion'
    ]]
    
    return df_final

# Ejecución principal
def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()
        
        print("\nExtrayendo datos de productos...")
        df_productos = extraer_productos(uid, models)
        print("Muestra de datos de productos:")
        print(df_productos.head())
        print(f"Total de productos: {len(df_productos)}")
        
        df_productos.to_csv('productos_odoo.csv', index=False)
        print("\nDatos guardados en: productos_odoo.csv")
        
    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()
