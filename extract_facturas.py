import xmlrpc.client
import pandas as pd

url = 'https://movingfood.konos.cl'
db = 'movingfood-mfood-erp-main-7481157'
username = 'logistica@movingfood.cl'
api_key = '7a1e4e24b1f34abbe7c6fd93fd5fd75dccda90a6'

def conectar_odoo():
    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, username, api_key, {})
    if not uid:
        raise Exception('Error de autenticación en Odoo')
    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    return uid, models

def extraer_facturas(uid, models, batch_size=100, max_records=5000):
    fields = [
        'id', 'name', 'move_type', 'invoice_date', 'partner_id', 'amount_total',
        'amount_residual', 'invoice_origin', 'invoice_payment_term_id',
        'currency_id', 'state', 'create_date', 'journal_id'
    ]

    offset = 0
    all_facturas = []

    while True:
        facturas = models.execute_kw(
            db, uid, api_key,
            'account.move', 'search_read',
            [[]],  # Sin filtros para traer todas
            {
                'fields': fields,
                'limit': batch_size,
                'offset': offset,
                'order': 'invoice_date desc'
            }
        )
        if not facturas:
            break
        all_facturas.extend(facturas)
        offset += batch_size

        print(f"Descargadas {len(all_facturas)} facturas...")

        if len(all_facturas) >= max_records:
            all_facturas = all_facturas[:max_records]  # Cortar a max_records
            break

    df = pd.DataFrame(all_facturas)

    # Procesar campos Many2one
    df['partner_id_name'] = df['partner_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['invoice_payment_term_id_name'] = df['invoice_payment_term_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['currency_id_name'] = df['currency_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)
    df['journal_id_name'] = df['journal_id'].apply(lambda v: v[1] if isinstance(v, (list, tuple)) and v else None)

    df = df.rename(columns={
        'id': 'id_documento',
        'name': 'numero',
        'move_type': 'tipo_documento',
        'invoice_date': 'fecha_emision',
        'amount_total': 'monto_total',
        'amount_residual': 'monto_pendiente',
        'invoice_origin': 'referencia_origen',
        'create_date': 'fecha_creacion',
        'partner_id_name': 'cliente',
        'invoice_payment_term_id_name': 'plazo_pago',
        'currency_id_name': 'moneda',
        'journal_id_name': 'diario',
        'state': 'estado'
    })

    estado_map = {
        'draft': 'Borrador',
        'posted': 'Publicado',
        'paid': 'Pagado',
        'cancel': 'Cancelado',
        'sent': 'Enviado'
    }
    df['estado'] = df['estado'].map(estado_map).fillna(df['estado'])

    columnas = [
        'id_documento', 'numero', 'tipo_documento', 'fecha_emision', 'cliente',
        'monto_total', 'monto_pendiente', 'referencia_origen', 'plazo_pago',
        'moneda', 'estado', 'fecha_creacion', 'diario'
    ]

    return df[columnas]

def main():
    try:
        print("Conectando a Odoo...")
        uid, models = conectar_odoo()

        print("Extrayendo facturas (hasta 5,000 más recientes)...")
        df_facturas = extraer_facturas(uid, models)

        print("\nMuestra de facturas extraídas:")
        print(df_facturas.head())

        print(f"\nTotal facturas extraídas: {len(df_facturas)}")

        df_facturas.to_csv('facturas_odoo.csv', index=False)
        print("Datos guardados en facturas_odoo.csv")

    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")

if __name__ == '__main__':
    main()
