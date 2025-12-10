from configparser import ConfigParser
from pathlib import Path

# def load_config(filename='database.ini', section='postgresql'):
#     parser = ConfigParser()
#     parser.read(filename)

#     config = {}
#     if parser.has_section(section):
#         params = parser.items(section)
#         for param in params:
#             config[param[0]] = param[1]
#     else:
#         raise Exception(f'Section {section} not found in the {filename} file')
#     return config

def load_config(filename: str = "database.ini", section: str = "postgresql") -> dict:
    base_dir = Path(__file__).resolve().parent
    config_path = base_dir / filename

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    parser = ConfigParser()
    parser.read(config_path)

    if not parser.has_section(section):
        raise Exception(
            f"Section [{section}] not found in {config_path}. "
            f"Existing sections: {parser.sections()}"
        )

    # Đọc key/value trong section
    config = {name: value for name, value in parser.items(section)}
    return config


if __name__ == '__main__':
    cfg = load_config()
    print(cfg)
