import json  # To read the JSON config file
import re  # To look for templated variables
from pathlib import Path  # To handle paths
import os

# Render the template
from jinja2 import (
    StrictUndefined,
    Template,
    UndefinedError,
)

def main():
    """Generates dag files based on the template.

    How to run the script: python generate_dags.py"""
    # Load the template
    with open("./template.py", "r", encoding="utf-8") as file:
        template_str = file.read()  # <<< CHANGED: no longer calling protect_undefineds()

    folder_configs = Path("./dag_configs/")
    print(f"folder_configs {folder_configs}")
    
    dags_dir = "./../dags"
    
    if not os.path.exists(dags_dir):
        os.mkdir(dags_dir)
    else:
        print(f"The directory {dags_dir} already exists.")
    
    # For each JSON path that matches the pattern "config_*.json"
    for path_config in folder_configs.glob("config_*.json"):
        # Read configuration
        with open(path_config, "r", encoding="utf-8") as file:
            config = json.load(file)

        # NO LONGER WRAPPING UNDEFINED EXPRESSIONS
        # template_str = protect_undefineds(unsafe_template_str, config)

        filename = f"{dags_dir}/{config['dag_name']}.py"
        content = Template(template_str).render(config)
        
        with open(filename, mode="w", encoding="utf-8") as file:
            file.write(content)
            print(f"Created {filename} from config: {path_config.name}...")

# We're no longer calling this, so it can stay as-is or be removed entirely.
def protect_undefineds(template_str: str, config: dict):
    pattern = re.compile(r"(\{\{[^\{]*\}\})")
    for j2_expression in set(pattern.findall(template_str)):
        try:
            Template(j2_expression, undefined=StrictUndefined).render(config)
        except UndefinedError:
            template_str = template_str.replace(
                j2_expression, f"{{% raw %}}{j2_expression}{{% endraw %}}"
            )
    return template_str

if __name__ == "__main__":
    main()
