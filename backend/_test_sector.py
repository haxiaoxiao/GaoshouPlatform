import xtquant.xtdata as xt
import time, threading

result = {'done': False, 'error': None}

def download():
    try:
        print('Starting download_sector_data...')
        xt.download_sector_data()
        result['done'] = True
        print('Done!')
    except Exception as e:
        result['error'] = str(e)
        print(f'Error: {e}')

t = threading.Thread(target=download, daemon=True)
t.start()
t.join(timeout=30)

if result['done']:
    stocks = xt.get_stock_list_in_sector('沪深A股')
    print(f'A-share stocks after download: {len(stocks)}')
elif result['error']:
    print('Failed:', result['error'])
else:
    print('TIMEOUT after 30s - download_sector_data is hanging')
    print('Trying get_instrument_detail...')
    info = xt.get_instrument_detail('600051.SH', iscomplete=False)
    print('600051.SH detail exists:', info is not None)