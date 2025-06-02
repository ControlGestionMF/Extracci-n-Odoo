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

# Función para extraer clientes (modelo res.partner)
def extraer_clientes(uid, models):
    fields = [
        'id', 'company_type', 'type', 'name', 'vat', 'visit_day',
        'street', 'street2', 'city', 'state_id', 'email', 'phone',
        'create_date', 'property_payment_term_id', 'credit_limit',
        'partner_latitude', 'partner_longitude', 'category_id', 'user_id'
    ]
    
    # Leer datos desde Odoo
    clientes = models.execute_kw(
        db, uid, api_key,
        'res.partner', 'search_read',
        [[],], {'fields': fields}
    )
    df = pd.DataFrame(clientes)

    # Procesar campos Many2one y listas
    for f in ['state_id', 'property_payment_term_id', 'user_id']:
        df[f + '_id'] = df[f].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
        df[f + '_name'] = df[f].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    
    # Función para procesar categorías de manera segura
    def procesar_etiquetas(cats):
        if not cats or not isinstance(cats, list):
            return ''
        try:
            # Extraer el segundo elemento (nombre) de cada tupla de categoría
            return ','.join([str(c[1]) for c in cats if isinstance(c, (list, tuple)) and len(c) > 1])
        except (TypeError, IndexError):
            return ''
    
    df['etiqueta'] = df['category_id'].apply(procesar_etiquetas)

    # Mapear columnas al esquema DataWarehouse
    df_dw = df.rename(columns={
        'id': 'id_cliente',
        'company_type': 'tipo_compania',
        'type': 'tipo_direccion',
        'name': 'nombre_cliente',
        'vat': 'rut',
        'visit_day': 'dia_visita',
        'street': 'calle1',
        'street2': 'calle2',
        'city': 'ciudad',
        'email': 'mail',
        'phone': 'telefono',
        'create_date': 'fecha_creacion',
        'credit_limit': 'credito_limite'
    })

    # Seleccionar y reordenar columnas
    df_final = df_dw[[
        'id_cliente', 'tipo_compania', 'tipo_direccion', 'nombre_cliente',
        'rut', 'dia_visita', 'calle1', 'calle2', 'state_id_name', 'ciudad',
        'mail', 'telefono', 'fecha_creacion', 'property_payment_term_id_name',
        'credito_limite', 'partner_latitude', 'partner_longitude', 'etiqueta', 'user_id_id'
    ]]

    # Renombrar campos restantes
    df_final.columns = [
        'id_cliente', 'tipo_compania', 'tipo_direccion', 'nombre_cliente',
        'rut', 'dia_visita', 'calle1', 'calle2', 'comuna', 'ciudad',
        'mail', 'telefono', 'fecha_creacion', 'plazo_pago',
        'credito_limite', 'geo_latitud', 'geo_longitud',
        'etiqueta', 'id_vendedor'
    ]
    
    return df_final

# Función para extraer productos (modelo product.product)
def extraer_productos(uid, models):
    fields = [
        'id', 'default_code', 'name', 'uom_id', 
        'list_price', 'standard_price', 'sale_ok', 'create_date'
    ]
    
    # Leer datos desde Odoo
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
        
        print("\nExtrayendo datos de clientes...")
        df_clientes = extraer_clientes(uid, models)
        print("Muestra de datos de clientes:")
        print(df_clientes.head())
        print(f"Total de clientes: {len(df_clientes)}")
        
        print("\nExtrayendo datos de productos...")
        df_productos = extraer_productos(uid, models)
        print("Muestra de datos de productos:")
        print(df_productos.head())
        print(f"Total de productos: {len(df_productos)}")
        
        # Guardar en archivos CSV
        df_clientes.to_csv('clientes_odoo.csv', index=False)
        df_productos.to_csv('productos_odoo.csv', index=False)
        print("\nDatos guardados en:")
        print("- clientes_odoo.csv")
        print("- productos_odoo.csv")
        
    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()