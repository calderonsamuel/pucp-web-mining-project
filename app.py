from shiny import App, reactive, render, ui
import polars as pl

ordenes = pl.read_parquet('data/ordenes.parquet')

choices_anno = ordenes['in_orden_anno'].unique().sort().to_list()

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.input_select(
            id="year",
            label="Año",
            choices=choices_anno
        ),
        ui.input_select(
            id="entidad",
            label="Entidad",
            choices=[]
        )
    ),
    ui.layout_columns(
        ui.value_box(
            title="Total de ordenes",
            value = 1000
        ),
        ui.value_box(
            title="Gasto total",
            value = 1000
        ),
        ui.value_box(
            title="Gasto total",
            value = 1000
        )            
    ),
    ui.layout_columns(
        ui.card(
            ui.card_header("Tabla"),
            ui.output_data_frame("table")
        )
    ),
    title="Dashboard",
    fillable=True
)

def server(input, output, session):
    @reactive.calc
    def data_filtered_year():
        return ordenes.filter(pl.col("in_orden_anno") == input.year())
    
    @reactive.calc
    def data_filtered_entidad():
        return data_filtered_year().filter(pl.col("entidad_nombre") == input.entidad())


    @reactive.effect
    def observe_year():
        choices = data_filtered_year()["entidad_nombre"].unique().sort().to_list()
        ui.update_select("entidad", choices=choices)

    @render.data_frame
    def table():
        return data_filtered_entidad()
    

app = App(app_ui, server)
