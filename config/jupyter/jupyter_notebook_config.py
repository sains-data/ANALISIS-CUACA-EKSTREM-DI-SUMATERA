# Jupyter Notebook Configuration
c = get_config()

# Allow connections from any IP
c.NotebookApp.ip = '0.0.0.0'
c.NotebookApp.port = 8888
c.NotebookApp.open_browser = False
c.NotebookApp.allow_root = True

# Disable token authentication for development
c.NotebookApp.token = ''
c.NotebookApp.password = ''

# Set the notebook directory
c.NotebookApp.notebook_dir = '/workspace'

# Enable extensions
c.NotebookApp.nbserver_extensions = {
    'jupyter_nbextensions_configurator': True,
}

# Allow CORS for development
c.NotebookApp.allow_origin = '*'
c.NotebookApp.allow_credentials = True
