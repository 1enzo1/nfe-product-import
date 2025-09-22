from pathlib import Path
try:
    from lxml import etree
except Exception as e:
    print('lxml not available:', e)
    raise
root = Path('example_docs')
counts = {}
for p in root.glob('*.xml'):
    tree = etree.parse(str(p))
    r = tree.getroot()
    ns = dict(r.nsmap)
    if None in ns:
        ns['nfe'] = ns.pop(None)
    if 'nfe' not in ns:
        ns['nfe'] = 'http://www.portalfiscal.inf.br/nfe'
    cnt = len(r.findall('.//nfe:det', namespaces=ns))
    counts[p.name] = cnt
print(counts)
print('total', sum(counts.values()))
