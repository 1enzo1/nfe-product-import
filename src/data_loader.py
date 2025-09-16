import pandas as pd
from lxml import etree
import yaml

def load_config(config_path="config.yaml"):
    """Loads the YAML configuration file."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def load_nfe_xml(file_path: str):
    """
    Parses an NF-e XML file and extracts item data.

    Args:
        file_path: The path to the .xml file.

    Returns:
        A list of dictionaries, where each dictionary represents an item
        with its relevant fields.
    """
    try:
        # Parse the XML file with a parser that can handle different encodings
        parser = etree.XMLParser(encoding='utf-8', recover=True)
        tree = etree.parse(file_path, parser=parser)
        root = tree.getroot()

        # Namespace is usually defined in the root element, find it
        # The format is {http://www.portalfiscal.inf.br/nfe}nfeProc
        ns = {'nfe': root.tag.split('}')[0][1:]}

        items = []
        # Find all 'det' elements, which represent the items
        for det in root.findall('.//nfe:det', namespaces=ns):
            prod = det.find('nfe:prod', namespaces=ns)

            item = {
                'cProd': prod.find('nfe:cProd', namespaces=ns).text if prod.find('nfe:cProd', namespaces=ns) is not None else None,
                'xProd': prod.find('nfe:xProd', namespaces=ns).text if prod.find('nfe:xProd', namespaces=ns) is not None else None,
                'cEAN': prod.find('nfe:cEAN', namespaces=ns).text if prod.find('nfe:cEAN', namespaces=ns) is not None else None,
                'NCM': prod.find('nfe:NCM', namespaces=ns).text if prod.find('nfe:NCM', namespaces=ns) is not None else None,
                'CEST': prod.find('nfe:CEST', namespaces=ns).text if prod.find('nfe:CEST', namespaces=ns) is not None else None,
                'CFOP': prod.find('nfe:CFOP', namespaces=ns).text if prod.find('nfe:CFOP', namespaces=ns) is not None else None,
                'uCom': prod.find('nfe:uCom', namespaces=ns).text if prod.find('nfe:uCom', namespaces=ns) is not None else None,
                'qCom': float(prod.find('nfe:qCom', namespaces=ns).text) if prod.find('nfe:qCom', namespaces=ns) is not None else 0.0,
                'vUnCom': float(prod.find('nfe:vUnCom', namespaces=ns).text) if prod.find('nfe:vUnCom', namespaces=ns) is not None else 0.0,
                'vProd': float(prod.find('nfe:vProd', namespaces=ns).text) if prod.find('nfe:vProd', namespaces=ns) is not None else 0.0,
            }
            items.append(item)

        return items

    except Exception as e:
        print(f"Error parsing XML file {file_path}: {e}")
        return []

def load_master_data(file_path: str, sheet_name=0):
    """
    Loads the master data from the specified Excel file.

    Args:
        file_path: The path to the .xlsx file.
        sheet_name: The name or index of the sheet to read.

    Returns:
        A pandas DataFrame containing the master data.
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
        # Normalize column names for easier access
        df.columns = [str(col).strip().lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        # It's crucial to handle potential empty rows or weird data types from Excel
        df = df.dropna(how='all')
        # Ensure key columns are of the correct type, handling errors by coercing to NaN
        if 'sku' in df.columns:
            df['sku'] = df['sku'].astype(str)
        if 'barcode' in df.columns:
            df['barcode'] = pd.to_numeric(df['barcode'], errors='coerce').astype('Int64').astype(str)
        return df
    except FileNotFoundError:
        print(f"Error: Master data file not found at {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error loading master data from {file_path}: {e}")
        return pd.DataFrame()
