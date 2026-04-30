import xtquant.xtdata as xt
import threading, time

# Test download_history_contracts
print('Testing download_history_contracts...')
t = threading.Thread(target=xt.download_history_contracts, daemon=True)
t.start()
t.join(timeout=10)
if t.is_alive():
    print('TIMEOUT - download_history_contracts hung')
else:
    print('download_history_contracts completed')

# Try A-share stocks  
sector_list = xt.get_sector_list()
a_stocks = set()
for s in sector_list:
    if s.startswith('SW1') and '\u6743' not in s:  # exclude options
        try:
            stocks = xt.get_stock_list_in_sector(s)
            for st in stocks:
                if st.endswith(('.SH', '.SZ')):
                    code = st[:6]
                    if code.startswith(('60', '00', '30', '68')):
                        a_stocks.add(st)
        except:
            pass

print(f'A-stock count from SW sectors: {len(a_stocks)}')
print('First 5:', sorted(list(a_stocks))[:5])