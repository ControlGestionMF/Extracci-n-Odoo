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

# Función para extraer pedidos de venta
def extraer_pedidos_venta(uid, models):
    fields = [
        'id', 'name', 'date_order', 'partner_id', 'user_id', 'amount_total',
        'state', 'invoice_status', 'pricelist_id', 'payment_term_id',
        'client_order_ref', 'validity_date'
    ]

    pedidos = models.execute_kw(
        db, uid, api_key,
        'sale.order', 'search_read',
        [[],], {'fields': fields}
    )
    df = pd.DataFrame(pedidos)

    # Procesar campos Many2one
    for campo in ['partner_id', 'user_id', 'pricelist_id', 'payment_term_id']:
        df[campo + '_id'] = df[campo].apply(lambda v: v[0] if isinstance(v, (list, tuple)) and v else None)
        df[campo + '_name'] = df[campo].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)

    # Renombrar columnas al formato deseado
    df_final = df.rename(columns={
        'id': 'id_pedido',
        'name': 'nombre_pedido',
        'date_order': 'fecha_pedido',
        'partner_id_name': 'cliente',
        'user_id_name': 'vendedor',
        'amount_total': 'monto_total',
        'state': 'estado_pedido',
        'invoice_status': 'estado_factura',
        'pricelist_id_name': 'lista_precios',
        'payment_term_id_name': 'plazo_pago',
        'client_order_ref': 'referencia_cliente',
        'validity_date': 'fecha_validez'
    })

    # Filtrar solo columnas relevantes
    columnas = [
        'id_pedido', 'nombre_pedido', 'fecha_pedido', 'cliente',
        'vendedor', 'monto_total', 'estado_pedido', 'estado_factura',
        'lista_precios', 'plazo_pago', 'referencia_cliente', 'fecha_validez'
    ]
    return df_final[columnas]

# Ejecución principal
def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()

        print("Extrayendo pedidos de venta...")
        df_pedidos = extraer_pedidos_venta(uid, models)

        print("\nMuestra de los pedidos extraídos:")
        print(df_pedidos.head())

        print("\nResumen de datos:")
        print(f"Total de pedidos: {len(df_pedidos)}")
        print(f"Columnas: {list(df_pedidos.columns)}")

        # Guardar en CSV
        df_pedidos.to_csv('pedidos_venta_odoo.csv', index=False)
        print("Datos guardados en pedidos_venta_odoo.csv")

    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()
