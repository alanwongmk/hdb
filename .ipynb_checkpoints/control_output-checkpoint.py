from IPython.display import HTML


css = HTML("""
<style>

/* JupyterLab output font size (print, text output) */
.jp-OutputArea-output pre {
    font-size: 12px !important;
}

/* Pandas DataFrame font size */
table.dataframe td, table.dataframe th {
    font-size: 10px !important;
}

</style>
""")